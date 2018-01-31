#!/usr/bin/env python
# -*- coding: utf-8 -*-

##############################################################################
#  Copyright Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##############################################################################

import json
import os
import six
from six.moves import urllib

from girder import config
from girder.constants import AssetstoreType, SettingKey
from girder.exceptions import GirderException
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.models.folder import Folder
from girder.models.group import Group
from girder.models.item import Item
from girder.models.setting import Setting
from girder.models.user import User
from girder.utility import assetstore_utilities, progress
from tests import base

# boiler plate to start and stop the server

os.environ['GIRDER_PORT'] = os.environ.get('GIRDER_TEST_PORT', '20201')
config.loadConfig()  # Must reload config to pickup correct port


def setUpModule():
    base.enabledPlugins.append('database_assetstore')
    base.startServer(False)


def tearDownModule():
    base.stopServer()


class AssetstoreTest(base.TestCase):
    def setUp(self):
        base.TestCase.setUp(self)

        users = ({
            'email': 'good@email.com',
            'login': 'goodlogin',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser2@email.com',
            'login': 'regularuser2',
            'firstName': 'Second',
            'lastName': 'Last',
            'password': 'goodpassword'
        })
        self.admin, self.user, self.user2 = [
            User().createUser(**user) for user in users]
        self.group1 = Group().createGroup('Group 1', self.admin)
        Group().addUser(self.group1, self.user)
        self.group2 = Group().createGroup('Group 2', self.admin)
        folders = Folder().childFolders(
            self.admin, 'user', user=self.admin)
        for folder in folders:
            if folder['name'] == 'Public':
                self.publicFolder = folder
            elif folder['name'] == 'Private':
                self.privateFolder = folder
        self.item1 = Item().createItem(
            'item1', creator=self.admin, folder=self.publicFolder)
        self.item2 = Item().createItem(
            'item2', creator=self.admin, folder=self.publicFolder)
        # Define locations for the default assetstores
        self.dbParams = {
            'type': AssetstoreType.DATABASE,
            'name': 'Test Assetstore',
            'dbtype': 'sqlalchemy_postgres',
            'dburi': os.environ.get(
                'GIRDER_DATABASE_ASSETSTORE_POSTGRES_DB',
                'postgresql://postgres@127.0.0.1/sampledb'),
        }
        self.dbParams2 = {
            'type': AssetstoreType.DATABASE,
            'name': 'All Mongo Assetstores',
            'dburi': '/'.join(os.environ.get('GIRDER_TEST_DB').split('/')[:-1])
        }
        self.dbParams3 = {
            'type': AssetstoreType.DATABASE,
            'name': 'One Mongo Assetstores',
            'dburi': os.environ.get('GIRDER_TEST_DB')
        }

    def _createTownItem(self, params=None, userAssetstore=False):
        from girder.plugins.database_assetstore.base import PluginSettings

        if not params:
            params = {}
        if not userAssetstore:
            # Create assetstore
            resp = self.request(path='/assetstore', method='POST', user=self.admin,
                                params=self.dbParams)
            self.assertStatusOk(resp)
            assetstore1 = resp.json
            assetstoreId = str(assetstore1['_id'])
        else:
            Setting().set(PluginSettings.USER_DATABASES, True)
            assetstore1 = None
            assetstoreId = 'user'
            if 'uri' not in params:
                params['uri'] = self.dbParams['dburi']
        # Import a table from postgres
        params.update({
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': 'towns',
        })
        resp = self.request(
            path='/database_assetstore/%s/import' % assetstoreId,
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        townItem = list(Item().textSearch('towns', user=self.admin, limit=1))[0]
        townFile = list(Item().childFiles(item=townItem))[0]
        return townItem, townFile, assetstore1

    def testAssetstoreCreate(self):
        # Make sure admin access required
        resp = self.request(path='/assetstore', method='POST',
                            params=self.dbParams, user=self.user)
        self.assertStatus(resp, 403)
        # Test validation
        altparams = self.dbParams.copy()
        altparams.pop('dburi')
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=altparams)
        self.assertStatus(resp, 400)
        self.assertEqual(resp.json['message'], 'Missing uri field.')
        altparams = self.dbParams.copy()
        altparams.pop('dbtype')
        altparams['dburi'] = os.environ.get('postgres@127.0.0.1/sampledb')
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=altparams)
        self.assertStatus(resp, 400)
        self.assertIn('specify dbtype', resp.json['message'])
        altparams = self.dbParams.copy()
        altparams['dburi'] = os.environ.get('postgres@127.0.0.1/')
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=altparams)
        self.assertStatus(resp, 400)
        self.assertIn('URI is not valid', resp.json['message'])
        # Create the assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore = resp.json
        # Update the assetstore
        altparams = self.dbParams.copy()
        altparams['name'] = 'Another name'
        altparams['current'] = False
        resp = self.request(path='/assetstore/' + str(assetstore['_id']),
                            method='PUT', user=self.admin, params=altparams)
        self.assertStatusOk(resp)
        # Test update failures
        resp = self.request(path='/assetstore/' + str(assetstore['_id']),
                            method='PUT', user=self.user, params=altparams)
        self.assertStatus(resp, 403)
        altparams = self.dbParams.copy()
        altparams['dburi'] = os.environ.get('postgres@127.0.0.1/')
        altparams['current'] = False
        resp = self.request(path='/assetstore/' + str(assetstore['_id']),
                            method='PUT', user=self.admin, params=altparams)
        self.assertStatus(resp, 400)
        self.assertIn('URI is not valid for dbtype', resp.json['message'])

    def testAssetstoreTablesAndImports(self):
        # Create assetstores
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams2)
        self.assertStatusOk(resp)
        assetstore2 = resp.json
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams3)
        self.assertStatusOk(resp)
        assetstore3 = resp.json
        # Get a list of tables
        resp = self.request(
            path='/database_assetstore/%s/tables' % str(assetstore1['_id']),
            user=self.user)
        self.assertStatus(resp, 403)
        resp = self.request(
            path='/database_assetstore/%s/tables' % str(assetstore1['_id']),
            user=self.admin)
        self.assertStatusOk(resp)
        tables1 = resp.json
        self.assertEqual(len(tables1), 1)
        self.assertTrue(len([table for table in tables1[0]['tables']
                             if table['table'] == 'towns']))
        resp = self.request(
            path='/database_assetstore/%s/tables' % str(assetstore2['_id']),
            user=self.admin)
        self.assertStatusOk(resp)
        tables2 = resp.json
        self.assertGreater(len(tables2), 1)
        resp = self.request(
            path='/database_assetstore/%s/tables' % str(assetstore3['_id']),
            user=self.admin)
        self.assertStatusOk(resp)
        tables3 = resp.json
        self.assertEqual(len(tables3), 1)

        # Test imports
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'not a folder',
            'table': '[not a list'
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.user, params=params)
        self.assertStatus(resp, 403)
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid parentType', resp.json['message'])
        params['parentType'] = 'folder'
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('JSON list', resp.json['message'])
        params['table'] = json.dumps({'not a list': []})
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('JSON list', resp.json['message'])
        params['table'] = json.dumps([])
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have at least one value', resp.json['message'])
        params['table'] = json.dumps([''])
        params['format'] = 'not a format'
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Format must be one of', resp.json['message'])
        del params['format']

        # Import some tables from postgres
        params['table'] = json.dumps([
            'towns', {'table': 'edges', 'schema': 'tiger'}])
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Doing it again should be fine
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Asking for just one table we already have should also be fine.
        params['table'] = 'towns'
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Setting a bad limit should throw an error
        params['limit'] = 'not an int'
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be an integer', resp.json['message'])
        # But an empty or 'none' limit should be okay
        params['limit'] = ''
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        params['limit'] = 'none'
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        del params['limit']

        # Import a table from mongo
        params['table'] = json.dumps([{
            'table': 'user',
            'database': os.environ.get('GIRDER_TEST_DB').split('/')[-1]
        }])
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore2['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Doing it again should be fine
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore2['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Import a database from mongo
        params['table'] = json.dumps([{
            'database': os.environ.get('GIRDER_TEST_DB').split('/')[-1]
        }])
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore2['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Doing it again should be fine
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore2['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)

        # Test some direct calls to the importData method to tests some
        # additional error conditions
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)
        with self.assertRaises(GirderException):
            adapter.importData(
                self.publicFolder, 'folder',
                {'tables': ['towns'], 'limit': 'not an int'},
                progress.noProgress, self.admin)
        self.assertEqual(len(list(Item().textSearch('towns', user=self.admin, limit=1))), 0)
        adapter.importData(
            self.publicFolder, 'folder', {'tables': ['towns']},
            progress.noProgress, self.admin)
        # Change the file we made for towns to remove the marker that it was
        # imported to prvent import from updating it.
        townItem = list(Item().textSearch('towns', user=self.admin, limit=1))[0]
        townFile = list(Item().childFiles(item=townItem))[0]
        del townFile['databaseMetadata']['imported']
        File().save(townFile)
        with self.assertRaises(GirderException):
            adapter.importData(
                self.publicFolder, 'folder', {'tables': ['towns']},
                progress.noProgress, self.admin)

    def testAssetstoreReadOnly(self):
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        # Import some tables from postgres
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': json.dumps([
                'towns', {'table': 'edges', 'schema': 'tiger'}]),
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        # Mark the towns database as not imported
        townItem = list(Item().textSearch('towns', user=self.admin, limit=1))[0]
        townFile = list(Item().childFiles(item=townItem))[0]
        del townFile['databaseMetadata']['imported']
        File().save(townFile)
        # We shouldn't be allowed to delete towns
        with six.assertRaisesRegex(self, Exception,
                                   'Database assetstores are read only'):
            resp = self.request(path='/item/%s' % str(townItem['_id']),
                                method='DELETE', user=self.admin)
        # If we remark it as imported, we can
        townFile['databaseMetadata']['imported'] = True
        File().save(townFile)
        resp = self.request(path='/item/%s' % str(townItem['_id']),
                            method='DELETE', user=self.admin)
        self.assertStatusOk(resp)
        # We can't upload
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)
        with six.assertRaisesRegex(self, Exception,
                                   'Database assetstores are read only'):
            adapter.initUpload({})
        with six.assertRaisesRegex(self, Exception,
                                   'Database assetstores are read only'):
            adapter.finalizeUpload({}, {})

    def testAssetstoreDownload(self):
        from girder.plugins.database_assetstore import assetstore
        from girder.plugins.database_assetstore import query

        for userAssetstore in (False, True):
            townItem, townFile, assetstore1 = self._createTownItem({
                'format': 'list',
                'fields': 'town,pop2010',
                'limit': '10'
            }, userAssetstore)

            resp = self.request(path='/item/%s/download' % str(townItem['_id']))
            self.assertStatusOk(resp)
            data = resp.json
            self.assertEqual(data['datacount'], 10)
            self.assertEqual(data['fields'], ['town', 'pop2010'])
            # Test extraParameters for format
            params = {
                'extraParameters': urllib.parse.urlencode({
                    'format': 'csv',
                    'limit': 5
                }),
                'contentDisposition': 'inline'
            }
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params,
                isJson=False)
            self.assertStatusOk(resp)
            data = self.getBody(resp)
            self.assertEqual(len(data.split('\r\n')), 7)
            self.assertEqual(data.split('\r\n', 1)[0], 'town,pop2010')
            # Test range requests
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params,
                isJson=False, additionalHeaders=[('Range', 'bytes=10-19')])
            self.assertStatus(resp, 206)
            self.assertEqual(self.getBody(resp), data[10:20])
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params,
                isJson=False, additionalHeaders=[('Range', 'bytes=50-')])
            self.assertStatus(resp, 206)
            self.assertEqual(self.getBody(resp), data[50:])
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params,
                isJson=False, additionalHeaders=[('Range', 'bytes=5000-')])
            self.assertStatus(resp, 206)
            self.assertEqual(self.getBody(resp), '')
            # Test more complex extraParameters
            extra = {
                'format': 'list',
                'fields': json.dumps(['town', 'pop2000', 'pop2010']),
                'sort': json.dumps([['pop2000', -1]]),
                'filters': json.dumps([{
                    'field': 'pop2000', 'operator': '<', 'value': 100000}]),
                'limit': 5
            }
            params = {'extraParameters': urllib.parse.urlencode(extra)}
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params)
            self.assertStatusOk(resp)
            data = resp.json
            self.assertEqual(data['datacount'], 5)
            self.assertEqual(data['fields'], ['town', 'pop2000', 'pop2010'])
            self.assertLess(int(data['data'][0][1]), 100000)
            self.assertLess(int(data['data'][1][1]), int(data['data'][0][1]))
            # Test with JSON extraParameters
            params = {'extraParameters': json.dumps(extra)}
            resp = self.request(
                path='/item/%s/download' % str(townItem['_id']), params=params)
            self.assertStatusOk(resp)
            data = resp.json
            self.assertEqual(data['datacount'], 5)
            self.assertEqual(data['fields'], ['town', 'pop2000', 'pop2010'])
            self.assertLess(int(data['data'][0][1]), 100000)
            self.assertLess(int(data['data'][1][1]), int(data['data'][0][1]))
            # Test a direct call
            townFile = list(Item().childFiles(item=townItem))[0]
            adapter = File().getAssetstoreAdapter(townFile)
            params = {
                'format': 'list',
                'fields': ['town', 'pop2000', 'pop2010'],
                'sort': [['pop2000', -1]],
                'filters': [{
                    'field': 'pop2000', 'operator': '<', 'value': 100000}],
                'limit': 5
            }
            func = adapter.downloadFile(townFile, headers=False,
                                        extraParameters=params)
            data = b''.join([part for part in func()])
            data = json.loads(data.decode('utf8'))
            self.assertEqual(data['datacount'], 5)
            self.assertEqual(data['fields'], ['town', 'pop2000', 'pop2010'])
            self.assertLess(int(data['data'][0][1]), 100000)
            self.assertLess(int(data['data'][1][1]), int(data['data'][0][1]))
            # Test a direct query with group
            params = {
                'format': 'rawlist',
                'sort': [
                    [{'func': 'count', 'param': {'field': 'town'}}, -1],
                    [{'func': 'max', 'param': {'field': 'town'}}, 1]
                ],
                'fields': [
                    {'func': 'max', 'param': {'field': 'town'}},
                    'pop2010',
                    {'func': 'count', 'param': {'field': 'town'}}
                ],
                'group': 'pop2010,popch80_90',
                'limit': 5,
            }
            data = query.queryDatabase(
                townFile['_id'], assetstore.getDbInfoForFile(townFile), params)
            data = list(data[0]())
            self.assertEqual(len(data), 5)
            self.assertEqual(data[0][0], 'ABINGTON')
            self.assertEqual(data[4][0], 'AGAWAM')

            # Test with bad extraParameters
            with six.assertRaisesRegex(self, Exception,
                                       'JSON-encoded dictionary, or a url'):
                adapter.downloadFile(townFile, headers=False, extraParameters=6)

            # Test with 0 and none limits
            params = {
                'format': 'list',
                'fields': 'town,pop2000',
                'sort': 'pop2000',
                'filters': json.dumps([{
                    'field': 'pop2000', 'operator': '>', 'value': 25000}]),
            }
            params['limit'] = 0
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            jsondata = b''.join([part for part in func()])
            data = json.loads(jsondata.decode('utf8'))
            self.assertEqual(data['datacount'], 0)
            self.assertEqual(data['fields'], ['town', 'pop2000'])
            # It shouldn't matter if we ask for this via json, query, or object
            func = adapter.downloadFile(
                townFile, headers=False,
                extraParameters=urllib.parse.urlencode(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=json.dumps(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)

            params['limit'] = 'none'
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            jsondata = b''.join([part for part in func()])
            data = json.loads(jsondata.decode('utf8'))
            self.assertEqual(data['datacount'], 71)
            self.assertEqual(data['fields'], ['town', 'pop2000'])
            # It shouldn't matter if we ask for this via json, query, or object
            func = adapter.downloadFile(
                townFile, headers=False,
                extraParameters=urllib.parse.urlencode(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=json.dumps(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)

            # None can also be used as unlimited
            params['limit'] = None
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            func = adapter.downloadFile(
                townFile, headers=False,
                extraParameters=urllib.parse.urlencode(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=json.dumps(params))
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            # filters can also be an object or tuple
            params['filters'] = json.loads(params['filters'])
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            self.assertEqual(b''.join([part for part in func()]), jsondata)
            params['filters'] = tuple(params['filters'])
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            self.assertEqual(b''.join([part for part in func()]), jsondata)

            # Test with group
            params['sort'] = [
                [{'func': 'count', 'param': {'field': 'town'}}, -1],
                [{'func': 'max', 'param': {'field': 'town'}}, 1]]
            params['fields'] = [
                {'func': 'max', 'param': {'field': 'town'}},
                'pop2010',
                {'func': 'count', 'param': {'field': 'town'}}]
            params['group'] = 'pop2010'
            params['limit'] = 5
            del params['filters']
            func = adapter.downloadFile(
                townFile, headers=False, extraParameters=params)
            jsondata = b''.join([part for part in func()])
            data = json.loads(jsondata.decode('utf8'))
            self.assertEqual(data['datacount'], 5)
            self.assertEqual(data['data'][0][0], 'DEDHAM')
            self.assertEqual(data['data'][0][2], 2)
            self.assertEqual(data['data'][4][0], 'ACTON')
            self.assertEqual(data['data'][4][2], 1)

    def testAssetstoreOpen(self):
        for userAssetstore in (False, True):
            townItem, townFile, assetstore1 = self._createTownItem(userAssetstore=userAssetstore)
            adapter = File().getAssetstoreAdapter(townFile)
            handle = adapter.open(townFile)
            data = handle.read(200)
            self.assertEqual(data[:2], b'{"')
            data += handle.read(200)
            self.assertNotEqual(data[:200], data[200:])
            handle.seek(100)
            data100 = handle.read(200)
            self.assertEqual(data100, data[100:300])
            handle.seek(-100, os.SEEK_END)
            dataend = handle.read(200)
            self.assertEqual(len(dataend), 100)
            self.assertNotEqual(dataend, data[:100])

    def testAssetstoreFileCopy(self):
        for userAssetstore in (False, True):
            townItem, townFile, assetstore1 = self._createTownItem(userAssetstore=userAssetstore)

            self.assertEqual(Item().childFiles(item=townItem).count(), 1)
            resp = self.request(path='/file/%s/copy' % str(townFile['_id']),
                                method='POST', user=self.admin,
                                params={'itemId': str(townItem['_id'])})
            self.assertStatusOk(resp)
            self.assertEqual(Item().childFiles(item=townItem).count(), 2)
            for file in Item().childFiles(item=townItem):
                File().remove(file)

    def testEmptyDirectQuery(self):
        from girder.plugins.database_assetstore import query
        # Test that queries fail with no connector
        with six.assertRaisesRegex(self, Exception,
                                   'Failed to connect'):
            query.queryDatabase(None, {}, {})

    def testInvalidParameters(self):
        # Test conditions that should return None
        from girder.plugins.database_assetstore import assetstore
        from girder.plugins.database_assetstore.base import DB_INFO_KEY, DB_ASSETSTORE_ID
        self.assertIsNone(assetstore.getDbInfoForFile({}))
        self.assertIsNone(assetstore.getDbInfoForFile(
            {DB_INFO_KEY: {}, 'assetstoreId': 'unknown'}, {'type': 'unknown'}))
        self.assertEqual(assetstore.getQueryParamsForFile({}), {})
        self.assertEqual(assetstore.getQueryParamsForFile(
            {DB_INFO_KEY: {'a': 'b'}}), {})
        self.assertEqual(assetstore.getQueryParamsForFile(
            {DB_INFO_KEY: {'sort': 'b'}}), {'sort': 'b'})
        # Test with non-database assetstore
        resp = self.request(path='/assetstore', method='GET', user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(2, len(resp.json))
        assetstore1 = resp.json[0]
        self.assertIsNone(assetstore.validateFile(
            {DB_INFO_KEY: {}, 'assetstoreId': str(assetstore1['_id'])}))
        # Test database validation
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams2)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        with six.assertRaisesRegex(self, Exception, 'must have a non-blank'):
            self.assertIsNone(assetstore.validateFile({
                DB_INFO_KEY: {'table': 'sample'},
                'assetstoreId': DB_ASSETSTORE_ID}))

    def testDisablingPluginWithActiveFiles(self):
        from girder.plugins.database_assetstore.base import validateSettings
        plugin_name = 'database_assetstore'
        # Create assetstores
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        event = (lambda: None)
        event.info = {
            'key': SettingKey.PLUGINS_ENABLED,
            'value': []}
        self.assertIsNone(validateSettings(event, plugin_name))
        self.assertEqual(event.info['value'], [])
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': 'towns'
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIsNone(validateSettings(event, plugin_name))
        self.assertEqual(event.info['value'], [plugin_name])
        townItem = list(Item().textSearch('towns', user=self.admin, limit=1))[0]
        resp = self.request(path='/item/%s' % str(townItem['_id']),
                            method='DELETE', user=self.admin)
        self.assertStatusOk(resp)
        event.info['value'] = []
        self.assertIsNone(validateSettings(event, plugin_name))
        self.assertEqual(event.info['value'], [])

    def testUnicodeInDownload(self):
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)

        def genDownload():
            yield u'\u0441\u0442\u0440\u043e\u043a\u0430 Unicode \U0001F603'

        file = {}
        newFunc, end = adapter._getDownloadSize(file, genDownload, 0, None)
        data = b''.join(newFunc())
        self.assertEqual(len(data), 25)
        self.assertEqual(file['size'], 25)
        self.assertEqual(end, 25)
        newFunc, end = adapter._getDownloadSize(file, genDownload, 3, 6)
        data = b''.join(newFunc())
        self.assertEqual(len(data), 3)
        self.assertEqual(data, b'\x82\xd1\x80')
        self.assertEqual(file['size'], 25)
        self.assertEqual(end, 6)

    def testAdapterGetTableList(self):
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)

        tableList = adapter.getTableList()
        tables = tableList[0]['tables']
        self.assertIn('towns', [table['name'] for table in tables])
        self.assertNotIn('information_schema.tables', [table['name'] for table in tables])
        tableList = adapter.getTableList(internalTables=True)
        tables = tableList[0]['tables']
        self.assertIn('towns', [table['name'] for table in tables])
        self.assertIn('information_schema.tables', [table['name'] for table in tables])

    def testAdapterConnectorForTable(self):
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)

        # This also tests the rawdict and rawlist output formats
        conn = adapter.getDBConnectorForTable('towns')
        query = adapter.queryDatabase(conn, {'filters': [['town', 'BOSTON']], 'format': 'rawdict'})
        data = list(query[0]())
        self.assertEqual(data[0]['town'], 'BOSTON')

        query = adapter.queryDatabase(conn, {
            'filters': [['town', 'BOSTON']],
            'fields': ['pop2010', 'town'],
            'format': 'rawlist'})
        data = list(query[0]())
        self.assertEqual(data[0][1], 'BOSTON')

        conn = adapter.getDBConnectorForTable('public.towns')
        query = adapter.queryDatabase(conn, {'filters': [['town', 'BOSTON']], 'format': 'rawdict'})
        data = list(query[0]())
        self.assertEqual(data[0]['town'], 'BOSTON')

    def testUserDatabasesSettings(self):
        from girder.plugins.database_assetstore import assetstore
        from girder.plugins.database_assetstore.base import PluginSettings

        Setting().set(PluginSettings.USER_DATABASES, 'false')
        self.assertIsNotNone(assetstore.checkUserImport(
            self.admin, 'postgresql://postgres@127.0.0.1/sampledb'))
        Setting().set(PluginSettings.USER_DATABASES, True)
        self.assertIsNone(assetstore.checkUserImport(
            self.admin, 'postgresql://postgres@127.0.0.1/sampledb'))
        with six.assertRaisesRegex(self, Exception, 'Invalid user database groups'):
            Setting().set(PluginSettings.USER_DATABASES_GROUPS, 'notinschema')
        Setting().set(PluginSettings.USER_DATABASES_GROUPS, [{
            'groupId': str(self.group1['_id']),
            'pattern': '^postgresql://.*'
        }, {
            'groupId': str(self.group2['_id']),
            'pattern': '.*'
        }])
        self.assertIsNone(assetstore.checkUserImport(
            self.admin, 'psql://postgres@127.0.0.1/sampledb'))
        self.assertIsNotNone(assetstore.checkUserImport(
            self.user, 'psql://postgres@127.0.0.1/sampledb'))
        self.assertIsNone(assetstore.checkUserImport(
            self.user, 'postgresql://postgres@127.0.0.1/sampledb'))
        self.assertIsNotNone(assetstore.checkUserImport(
            self.user2, 'postgresql://postgres@127.0.0.1/sampledb'))

    def testUpdateAssetstore(self):
        from girder.plugins.database_assetstore import assetstore, updateAssetstore

        event = (lambda: None)
        event.info = {
            'params': {'dbtype': assetstore.DB_ASSETSTORE_USER_TYPE},
            'assetstore': {
                'database': {'dbtype': None},
                'type': AssetstoreType.DATABASE}
        }
        updateAssetstore(event)
        self.assertEqual(event.info['assetstore']['database']['dbtype'],
                         assetstore.DB_ASSETSTORE_USER_TYPE)
        self.assertNotIn('uri', event.info['assetstore']['database'])

        uri = 'postgresql://postgres@127.0.0.1/sampledb'
        event = (lambda: None)
        event.info = {
            'params': {'dbtype': 'sqlalchemy_postgres', 'dburi': uri},
            'assetstore': {
                'database': {'dbtype': None, 'uri': None},
                'type': AssetstoreType.DATABASE
            }
        }
        updateAssetstore(event)
        self.assertEqual(event.info['assetstore']['database']['dbtype'], 'sqlalchemy_postgres')
        self.assertEqual(event.info['assetstore']['database']['uri'], uri)

    def testImportToItemAndFile(self):
        from girder.plugins.database_assetstore.base import DB_ASSETSTORE_ID

        userstore = Assetstore().load(DB_ASSETSTORE_ID)
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        for store in (assetstore1, userstore):
            adapter = assetstore_utilities.getAssetstoreAdapter(store)
            adapter.importData(self.item1, 'item', {
                'tables': ['towns'],
                'uri': self.dbParams['dburi'],
            }, progress.noProgress, self.admin)
            self.assertEqual(Item().childFiles(item=self.item1).count(), 1)
            file = Item().childFiles(item=self.item1)[0]
            adapter.importData(file, 'file', {
                'tables': ['towns'],
                'limit': 5,
                'uri': self.dbParams['dburi'],
            }, progress.noProgress, self.admin)
            self.assertEqual(Item().childFiles(item=self.item1).count(), 1)
            for file in Item().childFiles(item=self.item1):
                File().remove(file)

    def testAssetstoreUserTablesAndImports(self):
        from girder.plugins.database_assetstore.base import PluginSettings

        resp = self.request(
            path='/database_assetstore/user/tables', user=self.user,
            params={'uri': self.dbParams['dburi']})
        self.assertStatus(resp, 400)
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': 'towns',
            'uri': self.dbParams['dburi']
        }
        resp = self.request(
            path='/database_assetstore/user/import/allowed',
            method='GET', user=self.user, params=params)
        self.assertStatus(resp, 200)
        self.assertEqual(resp.json['allowed'], False)
        self.assertIn('imports are not allowed', resp.json['reason'])
        resp = self.request(
            path='/database_assetstore/user/import',
            method='PUT', user=self.user, params=params)
        self.assertStatus(resp, 400)

        # Enable user tables
        Setting().set(PluginSettings.USER_DATABASES, True)
        # A malformed uri should end up with an empty list
        resp = self.request(
            path='/database_assetstore/user/tables', user=self.user,
            params={'uri': 'malformed://uri/table'})
        self.assertStatusOk(resp)
        tables1 = resp.json
        self.assertEqual(len(tables1), 0)
        # Get a list of tables
        resp = self.request(
            path='/database_assetstore/user/tables', user=self.user,
            params={'uri': self.dbParams['dburi']})
        self.assertStatusOk(resp)
        tables1 = resp.json
        self.assertEqual(len(tables1), 1)
        self.assertTrue(len([table for table in tables1[0]['tables']
                             if table['table'] == 'towns']))
        resp = self.request(
            path='/database_assetstore/user/import/allowed',
            method='GET', user=self.user, params=params)
        self.assertStatus(resp, 200)
        self.assertEqual(resp.json['allowed'], True)
        self.assertNotIn('reason', resp.json)
        resp = self.request(
            path='/database_assetstore/user/import',
            method='PUT', user=self.user, params=params)
        self.assertStatusOk(resp)

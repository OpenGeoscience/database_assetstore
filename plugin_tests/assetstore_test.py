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
from girder.models.model_base import GirderException
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
        })
        self.admin, self.user = [
            self.model('user').createUser(**user) for user in users]
        folders = self.model('folder').childFolders(
            self.admin, 'user', user=self.admin)
        for folder in folders:
            if folder['name'] == 'Public':
                self.publicFolder = folder
            elif folder['name'] == 'Private':
                self.privateFolder = folder
        self.item1 = self.model('item').createItem(
            'item1', creator=self.admin, folder=self.publicFolder)
        self.item2 = self.model('item').createItem(
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
        altparams['dbtype'] = 'mongo'
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=altparams)
        self.assertStatus(resp, 400)
        self.assertIn('URI is not valid for dbtype', resp.json['message'])
        altparams = self.dbParams.copy()
        altparams['dburi'] = os.environ.get('postgres@127.0.0.1/')
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=altparams)
        self.assertStatus(resp, 400)
        self.assertIn('must include the database name', resp.json['message'])
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
        altparams['dbtype'] = 'mongo'
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
        # Change the file we made for towns to remove the marker that it was
        # imported to prvent import from updating it.
        townItem = list(self.model('item').textSearch('towns', user=self.admin,
                                                      limit=1))[0]
        townFile = list(self.model('item').childFiles(item=townItem))[0]
        del townFile['databaseMetadata']['imported']
        self.model('file').save(townFile)
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
        townItem = list(self.model('item').textSearch('towns', user=self.admin,
                                                      limit=1))[0]
        townFile = list(self.model('item').childFiles(item=townItem))[0]
        del townFile['databaseMetadata']['imported']
        self.model('file').save(townFile)
        # We shouldn't be allowed to delete towns
        with six.assertRaisesRegex(self, Exception,
                                   'Database assetstores are read only'):
            resp = self.request(path='/item/%s' % str(townItem['_id']),
                                method='DELETE', user=self.admin)
        # If we remark it as imported, we can
        townFile['databaseMetadata']['imported'] = True
        self.model('file').save(townFile)
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
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        # Import some tables from postgres
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': 'towns',
            'format': 'list',
            'fields': 'town,pop2010',
            'limit': '10'
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        townItem = list(self.model('item').textSearch(
            'towns', user=self.admin, limit=1))[0]
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
        townFile = list(self.model('item').childFiles(item=townItem))[0]
        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore1)
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
        data = json.loads(''.join([part for part in func()]))
        self.assertEqual(data['datacount'], 5)
        self.assertEqual(data['fields'], ['town', 'pop2000', 'pop2010'])
        self.assertLess(int(data['data'][0][1]), 100000)
        self.assertLess(int(data['data'][1][1]), int(data['data'][0][1]))
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
        jsondata = ''.join([part for part in func()])
        data = json.loads(jsondata)
        self.assertEqual(data['datacount'], 0)
        self.assertEqual(data['fields'], ['town', 'pop2000'])
        # It shouldn't matter if we ask for this via json, query, or object
        func = adapter.downloadFile(
            townFile, headers=False,
            extraParameters=urllib.parse.urlencode(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=json.dumps(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)

        params['limit'] = 'none'
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=params)
        jsondata = ''.join([part for part in func()])
        data = json.loads(jsondata)
        self.assertEqual(data['datacount'], 71)
        self.assertEqual(data['fields'], ['town', 'pop2000'])
        # It shouldn't matter if we ask for this via json, query, or object
        func = adapter.downloadFile(
            townFile, headers=False,
            extraParameters=urllib.parse.urlencode(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=json.dumps(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)

        # None can also be used as unlimited
        params['limit'] = None
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=params)
        self.assertEqual(''.join([part for part in func()]), jsondata)
        func = adapter.downloadFile(
            townFile, headers=False,
            extraParameters=urllib.parse.urlencode(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=json.dumps(params))
        self.assertEqual(''.join([part for part in func()]), jsondata)
        # filters can also be an object or tuple
        params['filters'] = json.loads(params['filters'])
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=params)
        self.assertEqual(''.join([part for part in func()]), jsondata)
        params['filters'] = tuple(params['filters'])
        func = adapter.downloadFile(
            townFile, headers=False, extraParameters=params)
        self.assertEqual(''.join([part for part in func()]), jsondata)

    def testAssetstoreFileCopy(self):
        # Create assetstore
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        # Import a tables from postgres
        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': 'towns',
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        townItem = list(self.model('item').textSearch('towns', user=self.admin,
                                                      limit=1))[0]
        townFile = list(self.model('item').childFiles(item=townItem))[0]
        self.assertEqual(self.model('item').childFiles(item=townItem).count(),
                         1)
        resp = self.request(path='/file/%s/copy' % str(townFile['_id']),
                            method='POST', user=self.admin,
                            params={'itemId': str(townItem['_id'])})
        self.assertStatusOk(resp)
        self.assertEqual(self.model('item').childFiles(item=townItem).count(),
                         2)

    def testEmptyDirectQuery(self):
        from girder.plugins.database_assetstore import query
        # Test that queries fail with no connector
        with six.assertRaisesRegex(self, Exception,
                                   'Failed to connect'):
            query.queryDatabase(None, {}, {})

    def testInvalidParameters(self):
        # Test conditions that should return None
        from girder.plugins.database_assetstore import assetstore
        from girder.plugins.database_assetstore.assetstore import DB_INFO_KEY
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
        self.assertEqual(1, len(resp.json))
        assetstore1 = resp.json[0]
        self.assertIsNone(assetstore.validateFile(
            {DB_INFO_KEY: {}, 'assetstoreId': str(assetstore1['_id'])}))
        # Test database validation
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams2)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        with six.assertRaisesRegex(self, Exception,
                                   'must have a non-blank database'):
            self.assertIsNone(assetstore.validateFile({
                DB_INFO_KEY: {'table': 'sample'},
                'assetstoreId': str(assetstore1['_id'])}))

    def testDisablingPluginWithActiveFiles(self):
        from girder.plugins.database_assetstore import validateSettings
        plugin_name = 'database_assetstore'
        # Create assetstores
        resp = self.request(path='/assetstore', method='POST', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        assetstore1 = resp.json
        event = (lambda: None)
        setattr(event, 'info', {
            'key': SettingKey.PLUGINS_ENABLED,
            'value': []})
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
        townItem = list(self.model('item').textSearch('towns', user=self.admin,
                                                      limit=1))[0]
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

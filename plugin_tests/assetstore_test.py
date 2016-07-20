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

from girder import config
from girder.constants import AssetstoreType
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
                'GIRDER_DB_ITEM_DB',
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
                            params=self.dbParams)
        self.assertStatus(resp, 401)
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
        params['limit'] = -4
        resp = self.request(
            path='/database_assetstore/%s/import' % str(assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('positive integer', resp.json['message'])
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
                {'tables': ['towns'], 'limit': -4},
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

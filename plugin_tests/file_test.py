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
import threading
import time

from girder import config
from tests import base

# boiler plate to start and stop the server

os.environ['GIRDER_PORT'] = os.environ.get('GIRDER_TEST_PORT', '20200')
config.loadConfig()  # Must reload config to pickup correct port


def setUpModule():
    base.enabledPlugins.append('database_assetstore')
    base.startServer(False)


def tearDownModule():
    base.stopServer()


def mergeDicts(*args):
    """
    Merge dictionaries.

    :params *args: any number of dictionaries.
    :returns: a merged dictionary
    """
    result = {}
    for item in args:
        result.update(item)
    return result


class FileTest(base.TestCase):
    dbParams = {
        'name': 'Assetstore 1',
        'type': 'database',  # AssetstoreType.DATABASE
        'dbtype': 'sqlalchemy_postgres',
        'dburi': os.environ.get('GIRDER_DATABASE_ASSETSTORE_POSTGRES_DB',
                                'postgresql://postgres@127.0.0.1/sampledb'),
    }

    def _setupDbFiles(self, args={}):
        """
        Set up db files, one using sqlalchemy_postgres, one sqlalchemy, and
        one not fully specified.  This creates a database assetstore for each
        of the first two.

        :param args: additional arguments to set on database connections.
        :returns: the three file ids.
        """
        dbParams = self.dbParams.copy()
        dbParams.update(args)
        resp = self.request(method='POST', path='/assetstore', user=self.admin,
                            params=dbParams)
        self.assertStatusOk(resp)
        self.assetstore1 = resp.json
        dbParams2 = dbParams.copy()
        dbParams2['name'] = 'Assetstore 2'
        dbParams2['dbtype'] = 'sqlalchemy'
        # dbParams2['dburi'] = 'sql://127.0.0.1/sampledb'
        dbParams2.update(args)
        resp = self.request(method='POST', path='/assetstore', user=self.admin,
                            params=dbParams2)
        self.assertStatusOk(resp)
        self.assetstore2 = resp.json

        from girder.plugins.database_assetstore.assetstore import DB_INFO_KEY
        self.file1 = self.model('file').createFile(
            name='file1', creator=self.admin, item=self.item1, size=0,
            assetstore=self.assetstore1, saveFile=False)
        self.file1[DB_INFO_KEY] = {'table': 'towns'}
        self.model('file').save(self.file1)
        self.file2 = self.model('file').createFile(
            name='file2', creator=self.admin, item=self.item2, size=0,
            assetstore=self.assetstore2, saveFile=False)
        self.file2[DB_INFO_KEY] = {'table': 'towns'}
        self.model('file').save(self.file2)

        self.file3 = self.model('file').createFile(
            name='file3', creator=self.admin, item=self.item1, size=0,
            assetstore=self.assetstore1, saveFile=True)

        fileId = str(self.file1['_id'])
        fileId2 = str(self.file2['_id'])
        fileId3 = str(self.file3['_id'])
        return fileId, fileId2, fileId3

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
        self.item1 = self.model('item').createItem(
            'item1', creator=self.admin, folder=self.publicFolder)
        self.item2 = self.model('item').createItem(
            'item2', creator=self.admin, folder=self.publicFolder)

    def testFileDatabaseEndpoints(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()

        resp = self.request(path='/file/notafile/database', user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/file/%s/database' % ('f' * len(fileId)),
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid file', resp.json['message'])
        resp = self.request(path='/file/%s/database' % fileId3, user=self.admin)
        self.assertStatusOk(resp)
        self.assertIsNone(resp.json)
        resp = self.request(path='/file/%s/database' % fileId3, user=self.user)
        self.assertStatusOk(resp)
        self.assertIs(resp.json, False)
        # Test the POST endpoint
        resp = self.request(method='POST', path='/file/notafile/database',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(method='POST', path='/file/%s/database' % (
            'f' * len(fileId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid file', resp.json['message'])
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId3, ), user=self.admin, type='application/json',
            body=json.dumps({}))
        self.assertStatus(resp, 400)
        self.assertIn('must have a non-blank table value',
                      resp.json['message'])
        params = {'table': 'towns', 'limit': 40}
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId3, ), user=self.admin, type='application/json',
            body=json.dumps(params))
        self.assertStatusOk(resp)
        resp = self.request(path='/file/%s/database' % fileId3, user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json, params)
        resp = self.request(path='/file/%s/database' % fileId3, user=self.user)
        self.assertStatusOk(resp)
        self.assertIs(resp.json, True)
        params['table'] = None
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId3, ), user=self.admin, type='application/json',
            body=json.dumps(params))
        self.assertStatus(resp, 400)
        self.assertIn('must have a non-blank table value',
                      resp.json['message'])
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId3, ), user=self.admin, type='application/json',
            body=json.dumps({'other': 'value'}))
        params['table'] = 'towns'
        params['other'] = 'value'
        resp = self.request(path='/file/%s/database' % fileId3, user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json, params)

    def testFileDatabaseBadConnectors(self):
        from girder.plugins.database_assetstore import dbs
        self.assertIsNone(dbs.getDBConnector('test1', {'type': 'base'}))
        dbs.base.registerConnectorClass('base', dbs.base.DatabaseConnector, {})
        with self.assertRaises(dbs.DatabaseConnectorException):
            dbs.getDBConnector('test1', {'type': 'base'})
        del dbs.base._connectorClasses['base']

        class ValidatingConnector(dbs.base.DatabaseConnector):
            def validate(self, *args, **kwargs):
                return True

        self.assertIsNone(dbs.getDBConnector('test1', {'type': 'validating'}))
        dbs.base.registerConnectorClass('validating', ValidatingConnector, {})
        self.assertIsNone(dbs.getDBConnector('test1', {'type': 'validating'}))
        del dbs.base._connectorClasses['validating']

    def testFileDatabaseBaseConnectorClass(self):
        from girder.plugins.database_assetstore import dbs

        # We have to subclass the base class and allow it to validate, or we
        # can't create an instance of the class.
        class ValidatingConnector(dbs.base.DatabaseConnector):
            def validate(self, *args, **kwargs):
                return True

        conn = ValidatingConnector()
        res = conn.performSelect()
        self.assertEqual(res['data'], [])
        self.assertEqual(res['fields'], [])
        self.assertFalse(super(ValidatingConnector, conn).validate())
        self.assertTrue(conn.checkOperatorDatatype('unknown', 'unknown'))
        self.assertEqual(conn.getTableList('ignore'), [])

    def testFileDatabaseFields(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()

        resp = self.request(path='/file/%s/database/fields' % (
            fileId3, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(path='/file/notafile/database/fields',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/file/%s/database/fields' % (
            'f' * len(fileId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid file', resp.json['message'])
        resp = self.request(path='/file/%s/database/fields' % (
            fileId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'town']) > 0)
        resp = self.request(path='/file/%s/database/fields' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'town']) > 0)
        # break the database link
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId, ), user=self.admin, type='application/json',
            body=json.dumps({'table': '_notpresent'}))
        self.assertStatusOk(resp)
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/fields' % (
                fileId, ), user=self.admin)

    def testFileDatabaseRefresh(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()

        resp = self.request(method='PUT', path='/file/%s/database/refresh' % (
            fileId3, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(
            method='PUT', path='/file/notafile/database/refresh',
            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(method='PUT', path='/file/%s/database/refresh' % (
            'f' * len(fileId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid file', resp.json['message'])
        resp = self.request(method='PUT', path='/file/%s/database/refresh' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], False)
        # Get fields so we will have something to refresh
        resp = self.request(path='/file/%s/database/fields' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        resp = self.request(method='PUT', path='/file/%s/database/refresh' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], True)
        resp = self.request(method='PUT', path='/file/%s/database/refresh' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], False)

    def testFileDatabaseView(self):
        # Test that we can get data from a view (this is the same as accessing
        # a table without a primary key)
        fileId, fileId2, fileId3 = self._setupDbFiles()

        params = {'table': 'geometry_columns'}
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId, ), user=self.admin, type='application/json',
            body=json.dumps(params))
        self.assertStatusOk(resp)
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertGreater(len(resp.json['data']), 10)
        self.assertGreater(resp.json['datacount'], 10)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))

    def testFileDatabaseSelectBasic(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()

        resp = self.request(path='/file/%s/database/select' % (
            fileId3, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(path='/file/notafile/database/select',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/file/%s/database/select' % (
            'f' * len(fileId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid file', resp.json['message'])

        # Test the default query
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 50)
        self.assertEqual(resp.json['datacount'], 50)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        # Test limit and offset using a basic sort
        params = {'sort': 'town', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['datacount'], 5)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        lastData = resp.json
        params['offset'] = 2
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][:-2], lastData['data'][2:])
        # break the database link
        resp = self.request(method='POST', path='/file/%s/database' % (
            fileId, ), user=self.admin, type='application/json',
            body=json.dumps({'table': '_notpresent'}))
        self.assertStatusOk(resp)
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId, ), user=self.admin, params=params)

    def testFileDatabaseSelectSort(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        # Test a variety of sorts
        params = {'sort': 'town', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        lastData = resp.json
        params = {'sort': 'town', 'sortdir': -1, 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][:1], lastData['data'][:1])
        self.assertGreater(resp.json['data'][0][resp.json['columns']['town']],
                           lastData['data'][0][lastData['columns']['town']])
        # Use a json sort specification
        params = {'sort': json.dumps(['town']), 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], lastData['data'])
        # This should work fine on file2
        resp = self.request(path='/file/%s/database/select' % (
            fileId2, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], lastData['data'])
        # Use a function
        params['sort'] = json.dumps([{
            'func': 'mod', 'param': [{'field': 'pop2010'}, 10]},
            ['town', -1]
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(
            int(resp.json['data'][0][resp.json['columns']['pop2010']]) % 10, 0)
        self.assertGreater(resp.json['data'][0][resp.json['columns']['town']],
                           resp.json['data'][1][resp.json['columns']['town']])
        # This must not work on file2
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId2, ), user=self.user, params=params)
        # Test with bad parameters
        params['sort'] = '["not valid json'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['sort'] = json.dumps({'not': ['a', 'list']})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['sort'] = 'unknownfield'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        params['sort'] = json.dumps([['town'], ['unknownfield', -1]])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])

    def testFileDatabaseSelectFields(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        # Unknown fields aren't allowed
        params = {'fields': 'unknown,town', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # a comma separated list works
        params['fields'] = 'town,pop2010'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # extra commas and white space at the ends of field names are allowed
        params['fields'] = 'town ,, pop2010 ,,'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # You can use json instead
        params['fields'] = json.dumps(['town', 'pop2010'])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # Invalid json fails
        params['fields'] = '["not valid json",town'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        # A zero-length list is all of the fields
        params['fields'] = json.dumps([])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertGreater(len(resp.json['fields']), 2)
        # instead of a field name, you can use a function
        params['fields'] = json.dumps([
            'town',
            {'func': 'mod', 'param': [{'field': 'pop2010'}, 10]},
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'town': 0, 'column_1': 1})
        # This must not work on file2
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId2, ), user=self.user, params=params)
        # We can use a reference to better find our column
        params['fields'] = json.dumps([
            'town',
            {
                'func': 'mod',
                'param': [{'field': 'pop2010'}, 10],
                'reference': 'popmod'
            },
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'town': 0, 'popmod': 1})
        # Distinct and count can always be used as functions
        # Distinct must be the first field
        params['fields'] = json.dumps([
            {'func': 'distinct', 'param': [{'field': 'pop2010'}]},
            'town',
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'town': 1, 'column_0': 0})
        # Count will return the tally of the distinct values
        params['fields'] = json.dumps([
            {'func': 'count', 'param': [{'func': 'distinct', 'param': [{'field': 'pop2010'}]}]},
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'column_0': 0})
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 348)
        # Test some function handling
        params['sort'] = 'town'
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'field': 'town'}}
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'abington')
        # This uses 'town' as a value in the first field, not a field
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': 'town'},
            'town'
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'town')
        # Function parameters must be fields, values, or other functions
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'unknown': 'town'}}
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # Fields in functions must exist
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'field': 'unknown'}}
        ])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # We don't have to use a function
        params['fields'] = json.dumps([{'field': 'town'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'ABINGTON')
        # But it needs to be a field or a function
        params['fields'] = json.dumps([{'unknown': 'town'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])

    def testFileDatabaseSelectFilterViaParams(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        # We can access filters either via the filter parameter or via the name
        # of each field optionally suffixed with different operators.
        baseParams = {'limit': 5, 'sort': 'town', 'fields': 'town'}
        # Exact match
        params = mergeDicts(baseParams, {'town': 'BOSTON'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params = mergeDicts(baseParams, {'town': 'boston'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)
        # minimum
        params = mergeDicts(baseParams, {'town_min': 'BOS'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # search
        params = mergeDicts(baseParams, {'town_search': '^bo.*n$'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 3)
        self.assertEqual(resp.json['data'][1][0], 'BOSTON')
        # compound
        params = mergeDicts(baseParams, {
            'town_min': 'BOS',
            'town_notsearch': '^bo.*n$'
        })
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][0][0], 'BOSTON')
        # numeric comparisons are sent as text
        params = mergeDicts(baseParams, {'pop2010_min': '150000'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 3)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # you can't use regex or search on numeric types
        params = mergeDicts(baseParams, {'pop2010_search': '150000'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Cannot use search operator on field',
                      resp.json['message'])
        # We should be able to get the same results regardless of whether we
        # use not or not_
        params = mergeDicts(baseParams, {
            'town_min': 'BOS',
            'town_notin': 'BOSTON'
        })
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOURNE')
        params = mergeDicts(baseParams, {
            'town_min': 'BOS',
            'town_not_in': 'BOSTON'
        })
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOURNE')

    def testFileDatabaseSelectFilters(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        params = {'limit': 5, 'sort': 'town', 'fields': 'town'}
        params['filters'] = '[not json'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['filters'] = json.dumps({'town': 'BOSTON'})
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['filters'] = json.dumps([{'town': 'BOSTON'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must specify a field or func', resp.json['message'])
        params['filters'] = json.dumps([{'field': 'town', 'value': 'BOSTON'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # Test have the value first
        params['filters'] = json.dumps([{
            'lvalue': 'BOSTON', 'value': {'field': 'town'}}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # test operators
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': '>=', 'value': 'BOS'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'gt', 'value': 'BOS'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'noop', 'value': 'BOS'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown filter operator', resp.json['message'])
        # Functions must be known
        params['filters'] = json.dumps([{
            'field': 'town', 'value': {'func': 'unknown', 'params': []}
        }])
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId, ), user=self.user, params=params)
        # We throw a different error when params is an empty dict
        params['filters'] = json.dumps([{
            'field': 'town', 'value': {'func': 'unknown', 'param': {}}}])
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId, ), user=self.user, params=params)
        # Test a filter composed of a list
        params['filters'] = json.dumps([['town', 'gt', 'BOS']])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'BOSTON']])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'gt', 'BOSTON', 'extra']])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have two or three components',
                      resp.json['message'])
        # Fail on an unknown field
        params['filters'] = json.dumps([['unknown', 'BOSTON']])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Filters must be on known fields', resp.json['message'])
        # Fail without a value
        params['filters'] = json.dumps([{
            'field': 'town'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have a value or rfunc', resp.json['message'])
        # Test a right function
        params['filters'] = json.dumps([{
            'field': 'town', 'rfunc': 'upper', 'rparam': 'boston'}])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # This must not work on file2
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId2, ), user=self.user, params=params)
        # Test a set of nested functions
        filters = [{
            'func': 'st_intersects',
            'param': [{
                'func': 'st_setsrid',
                'param': [{
                    'func': 'st_makepoint',
                    'param': [-72, 42.3601]
                }, 4326]
            }, {
                'func': 'st_transform',
                'param': [{
                    'field': 'geom'
                }, 4326]
            }],
            'operator': 'is',
            'value': True
        }]
        params['filters'] = json.dumps(filters)
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'RUTLAND')

    def testFileDatabaseSelectFormats(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        params = {
            'sort': 'town',
            'limit': 5,
            'fields': 'town,pop2010,shape_len,type'
        }
        params['fields'] = 'town,pop2010,shape_len,type'
        # Unknown format
        params['format'] = 'unknownFormat'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown output format', resp.json['message'])
        # List format
        params['format'] = 'list'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], [
            'town', 'pop2010', 'shape_len', 'type'])
        self.assertEqual(resp.json['columns'], {
            'town': 0, 'pop2010': 1, 'shape_len': 2, 'type': 3})
        self.assertTrue(isinstance(resp.json['data'][0], list))
        self.assertEqual(resp.json['data'][0][0], 'ABINGTON')
        # Dict format
        params['format'] = 'dict'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], [
            'town', 'pop2010', 'shape_len', 'type'])
        self.assertEqual(resp.json['columns'], {
            'town': 0, 'pop2010': 1, 'shape_len': 2, 'type': 3})
        self.assertTrue(isinstance(resp.json['data'][0], dict))
        self.assertEqual(set(resp.json['data'][0].keys()),
                         set(['town', 'pop2010', 'shape_len', 'type']))
        self.assertEqual(resp.json['data'][1]['town'], 'ACTON')
        # Capitalization doesn't matter
        params['format'] = 'DICT'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], [
            'town', 'pop2010', 'shape_len', 'type'])
        self.assertEqual(resp.json['columns'], {
            'town': 0, 'pop2010': 1, 'shape_len': 2, 'type': 3})
        self.assertTrue(isinstance(resp.json['data'][0], dict))
        self.assertEqual(set(resp.json['data'][0].keys()),
                         set(['town', 'pop2010', 'shape_len', 'type']))
        self.assertEqual(resp.json['data'][2]['town'], 'ACUSHNET')
        # csv format
        params['format'] = 'csv'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params, isJson=False)
        self.assertStatusOk(resp)
        data = self.getBody(resp)
        self.assertEqual(len(data.split('\r\n')), 7)
        self.assertEqual(data.split('\r\n')[0], params['fields'])
        self.assertEqual(data.split('\r\n')[4].split(',')[0], 'ADAMS')
        # JSON simple format
        params['format'] = 'json'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json[4]['town'], 'AGAWAM')
        # JSON Lines format
        params['format'] = 'JSON_Lines'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params, isJson=False)
        self.assertStatusOk(resp)
        data = self.getBody(resp)
        self.assertEqual(len(data.split('\n')), 6)
        self.assertEqual(set(json.loads(data.split('\n')[0]).keys()),
                         set(['town', 'pop2010', 'shape_len', 'type']))
        self.assertEqual(json.loads(data.split('\n')[0])['town'], 'ABINGTON')
        # GeoJSON format
        params['format'] = 'GeoJSON'
        geojsonfield = {'func': 'ST_AsGeoJSON', 'param': [{
            'func': 'st_transform', 'param': [{'field': 'geom'}, 4326]
        }]}
        params['fields'] = json.dumps([geojsonfield])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        geojson = resp.json
        self.assertTrue(isinstance(geojson, dict))
        self.assertEqual(geojson['type'], 'GeometryCollection')
        self.assertEqual(len(geojson['geometries']), 5)
        self.assertEqual(geojson['geometries'][0]['type'], 'MultiPolygon')
        self.assertIn('coordinates', geojson['geometries'][0])
        # We should discard the non-geojson fields
        params['fields'] = json.dumps(['town', geojsonfield, 'pop2010'])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json, geojson)
        params['fields'] = 'town,pop2010,shape_len,type'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['geometries']), 0)

    def testFileDatabaseSelectClient(self):
        fileId, fileId2, fileId3 = self._setupDbFiles()
        params = {'sort': 'town', 'limit': 1, 'clientid': 'test'}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        from girder.plugins.database_assetstore import dbs
        sessions = dbs.base._connectorCache[fileId].sessions
        # We should be tracking the a session for 'test'
        self.assertIn('test', sessions)
        self.assertFalse(sessions['test']['used'])
        last = sessions['test'].copy()
        # A new request should update the last used time
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertGreater(sessions['test']['last'], last['last'])
        self.assertEqual(sessions['test']['session'], last['session'])
        # Artifically age the last session and test that we get a new session
        last = sessions['test'].copy()
        sessions['test']['last'] -= 305  # 300 is the default expiry age
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(sessions['test']['session'], last['session'])
        # Send a slow query in a thread.  Use pg_sleep, as it produces more
        # consistent tests.  Before, we were using
        #   {'func': 'st_hausdorffdistance', 'param': [
        #       {'func': 'st_minimumboundingcircle', 'param': {
        #           'field': 'geom'}},
        #       {'field': 'geom'},
        #       0.03 + 0.01 * random.random()]},
        # whiched used a random number as part of the query to prevent
        # caching of the results.  This would occasionally fully process
        # instead of getting canceled.

        # Whitelist pg_sleep for this test
        from girder.plugins.database_assetstore import dbs, assetstore
        connector = dbs.getDBConnector(fileId, assetstore.getDbInfoForFile(
            self.file1))
        connector._allowedFunctions['pg_sleep'] = True

        slowParams = params.copy()
        slowParams['fields'] = json.dumps([
            'town',
            {'func': 'pg_sleep', 'param': [40]},
        ])
        slowParams['limit'] = 500
        slowResults = {}

        def slowQuery(params):
            try:
                self.request(path='/file/%s/database/select' % (
                    fileId, ), user=self.user, params=slowParams)
            except Exception as exc:
                slowResults['exc'] = repr(exc)

        slow = threading.Thread(target=slowQuery, kwargs={
            'params': params
        })
        slow.start()
        # Wait for the query to start
        while not sessions['test']['used'] and slow.is_alive():
            time.sleep(0.05)
        # Sending a normal request should cancel the slow one and respond
        # promptly
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        # The slow request should be cancelled
        slow.join()
        self.assertTrue(
            'canceling statement due to user' in slowResults['exc'] or
            'Internal server error' in slowResults['exc'] or
            'InterruptedException' in slowResults['exc'])

    def testFileDatabaseSelectPolling(self):
        # Create a test database connector so we can check polling
        from girder.plugins.database_assetstore import dbs

        dbInfo = {
            'queries': 0,
            'data': [[1]],
            'format': 'list'
        }

        class TestConnector(dbs.base.DatabaseConnector):
            name = 'test'

            def __init__(self, *args, **kwargs):
                super(TestConnector, self).__init__(*args, **kwargs)
                self.initialized = True

            def getFieldInfo(self):
                return [{'name': 'test', 'type': 'number'}]

            def performSelect(self, *args, **kwargs):
                dbInfo['queries'] += 1
                if dbInfo['data'] is None:
                    return
                results = super(TestConnector, self).performSelect(
                    *args, **kwargs)
                results['data'] = dbInfo['data']
                results['format'] = dbInfo['format']
                return results

            @staticmethod
            def validate(*args, **kwargs):
                return True

        dbs.base.registerConnectorClass(TestConnector.name, TestConnector, {})
        fileId, fileId2, fileId3 = self._setupDbFiles({
            'dbtype': 'test', 'dburi': 'test://nowhere/nowhere'})
        params = {}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        params = {'wait': 1}
        # Waiting shouldn't affect the results since there is data available
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        # If no data is available for the wait duration, we can get a null
        # response
        dbInfo['data'].pop()
        lastCount = dbInfo['queries']
        params = {'wait': 0.01, 'poll': 0.01}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json.get('data', []), [])
        self.assertEqual(dbInfo['queries'], lastCount + 2)
        # We should be able to wait for results

        def addData(delay, value):
            time.sleep(delay)
            dbInfo['data'].append([value])

        add = threading.Thread(target=addData, args=(1, 2))
        add.start()
        lastCount = dbInfo['queries']
        params = {'initwait': 0.3, 'poll': 0.1, 'wait': 10}
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        # Don't depend on exact counts, as the test could be slow
        self.assertEqual(resp.json['data'], [[2]])
        self.assertGreater(dbInfo['queries'], lastCount + 3)
        self.assertLess(dbInfo['queries'], lastCount + 9)
        add.join()

        # Test if we have bad data we get an exception
        dbInfo['data'] = None
        with self.assertRaises(Exception):
            resp = self.request(path='/file/%s/database/select' % (
                fileId, ), user=self.user, params=params)

        # Test that we can handle different data formats
        dbInfo['data'] = [{'test': 1}]
        dbInfo['format'] = 'dict'

        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params={'format': 'dict'})
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [{'test': 1}])

        dbInfo['data'] = [[1]]
        dbInfo['format'] = 'list'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user, params={'format': 'dict'})
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [{'test': 1}])

        dbInfo['data'] = [(1, )]
        dbInfo['format'] = 'unknown'
        resp = self.request(path='/file/%s/database/select' % (
            fileId, ), user=self.user)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown internal format', resp.json['message'])

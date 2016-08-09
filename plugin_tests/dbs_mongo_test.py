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


class DbsMongoTest(base.TestCase):
    dbParams = {
        'name': 'Assetstore 1',
        'type': 'database',  # AssetstoreType.DATABASE
        'dburi': os.environ.get('GIRDER_DATABASE_ASSETSTORE_MONGO_DB',
                                'mongodb://127.0.0.1/boston/permits'),
    }

    def setUp(self):
        base.TestCase.setUp(self)

        adminUser = {
            'email': 'good@email.com',
            'login': 'goodlogin',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }
        self.admin = self.model('user').createUser(**adminUser)
        folders = self.model('folder').childFolders(
            self.admin, 'user', user=self.admin)
        for folder in folders:
            if folder['name'] == 'Public':
                self.publicFolder = folder

        resp = self.request(method='POST', path='/assetstore', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        self.assetstore1 = resp.json

        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder'
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(
                self.assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.dbItem = list(self.model('item').textSearch(
            'permits', user=self.admin, limit=1))[0]
        self.dbFile = list(self.model('item').childFiles(item=self.dbItem))[0]
        self.dbFileId = str(self.dbFile['_id'])

    def testMongoDatabaseFields(self):
        resp = self.request(path='/file/%s/database/fields' % (
            self.dbFileId, ), user=self.admin)
        self.assertStatus(resp, 200)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'worktype']) > 0)

    def testMongoDatabaseSelectBasic(self):
        # Test the default query
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 50)
        self.assertEqual(resp.json['datacount'], 50)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        # Test limit and offset using a basic sort
        params = {'sort': 'issued_date', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['datacount'], 5)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        lastData = resp.json
        params['offset'] = 2
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][:-2], lastData['data'][2:])
        # Limit = 0
        params = {'sort': 'issued_date', 'limit': 0}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)

    def testMongoDatabaseSelectSort(self):
        params = {'sort': 'issued_date', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        lastData = resp.json
        params = {'sort': 'issued_date', 'sortdir': -1, 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][:1], lastData['data'][:1])
        self.assertGreater(
            resp.json['data'][0][resp.json['columns']['issued_date']]['$date'],
            lastData['data'][0][lastData['columns']['issued_date']]['$date'])

    def testMongoDatabaseSelectFields(self):
        # Unknown fields aren't allowed
        params = {'fields': 'unknown,zip', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # a comma separated list works
        params['fields'] = 'zip,comments'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['zip', 'comments'])
        self.assertEqual(resp.json['columns'], {'zip': 0, 'comments': 1})

    def testMongoDatabaseSelectFilters(self):
        params = {'limit': 5, 'sort': 'zip', 'fields': 'zip,comments'}
        params['filters'] = json.dumps([])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        params['filters'] = json.dumps([{'field': 'zip', 'value': '02133'}])
        params['limit'] = None
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 7)
        self.assertEqual(resp.json['data'][0][0], '02133')
        # Test have the value first -- this will fail
        params['limit'] = 10
        params['filters'] = json.dumps([{
            'lvalue': '02133', 'value': {'field': 'zip'}}])
        with six.assertRaisesRegex(self, Exception,
                                   'must use a known field as the left value'):
            resp = self.request(path='/file/%s/database/select' % (
                self.dbFileId, ), user=self.admin, params=params)
        # test operators
        params['filters'] = json.dumps([{
            'field': 'zip', 'operator': '>=', 'value': '02133'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 10)
        self.assertEqual(resp.json['data'][0][0], '02133')
        self.assertEqual(resp.json['data'][9][0], '02134')
        """
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'gt', 'value': 'BOS'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'noop', 'value': 'BOS'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown filter operator', resp.json['message'])
        # Test a filter composed of a list
        params['filters'] = json.dumps([['town', 'gt', 'BOS']])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'BOSTON']])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'gt', 'BOSTON', 'extra']])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have two or three components',
                      resp.json['message'])
        # Fail on an unknown field
        params['filters'] = json.dumps([['unknown', 'BOSTON']])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Filters must be on known fields', resp.json['message'])
        # Fail without a value
        params['filters'] = json.dumps([{
            'field': 'town'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have a value or rfunc', resp.json['message'])
        """

    def XtestMongoDatabaseSelectFormats(self):
        params = {
            'sort': 'town',
            'limit': 5,
            'fields': 'town,pop2010,shape_len,type'
        }
        params['fields'] = 'town,pop2010,shape_len,type'
        # Unknown format
        params['format'] = 'unknownFormat'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown output format', resp.json['message'])
        # List format
        params['format'] = 'list'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
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
            self.dbFileId, ), user=self.admin, params=params)
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
            self.dbFileId, ), user=self.admin, params=params)
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
            self.dbFileId, ), user=self.admin, params=params, isJson=False)
        self.assertStatusOk(resp)
        data = self.getBody(resp)
        self.assertEqual(len(data.split('\r\n')), 7)
        self.assertEqual(data.split('\r\n')[0], params['fields'])
        self.assertEqual(data.split('\r\n')[4].split(',')[0], 'ADAMS')
        # JSON simple format
        params['format'] = 'json'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json[4]['town'], 'AGAWAM')
        # JSON Lines format
        params['format'] = 'JSON_Lines'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params, isJson=False)
        self.assertStatusOk(resp)
        data = self.getBody(resp)
        self.assertEqual(len(data.split('\n')), 6)
        self.assertEqual(set(json.loads(data.split('\n')[0]).keys()),
                         set(['town', 'pop2010', 'shape_len', 'type']))
        self.assertEqual(json.loads(data.split('\n')[0])['town'], 'ABINGTON')

    def XtestMongoDatabaseSelectClient(self):
        params = {'sort': 'town', 'limit': 1, 'clientid': 'test'}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        from girder.plugins.database_assetstore import dbs
        sessions = dbs.base._connectorCache[self.dbFileId].sessions
        # We should be tracking the a session for 'test'
        self.assertIn('test', sessions)
        self.assertFalse(sessions['test']['used'])
        last = sessions['test'].copy()
        # A new request should update the last used time
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertGreater(sessions['test']['last'], last['last'])
        self.assertEqual(sessions['test']['session'], last['session'])
        # Artifically age the last session and test that we get a new session
        last = sessions['test'].copy()
        sessions['test']['last'] -= 305  # 300 is the default expiry age
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
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
                    self.dbFileId, ), user=self.admin, params=slowParams)
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
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        # The slow request should be cancelled
        slow.join()
        self.assertTrue(
            'canceling statement due to user' in slowResults['exc'] or
            'Internal server error' in slowResults['exc'] or
            'InterruptedException' in slowResults['exc'])

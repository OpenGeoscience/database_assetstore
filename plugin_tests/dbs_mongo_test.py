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
        params = {
            'limit': 5,
            'sort': 'zip',
            'fields': 'zip,comments,occupancytype'
        }
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
        params['sort'] = json.dumps(['zip', 'comments'])
        params['filters'] = json.dumps([{
            'field': 'comments', 'operator': 'regex', 'value': 'kitchen'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('kitchen', resp.json['data'][0][1])
        params['filters'] = json.dumps([{
            'field': 'comments', 'operator': 'not_regex', 'value': 'kitchen'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('kitchen', resp.json['data'][0][1])
        params['filters'] = json.dumps([{
            'field': 'comments', 'operator': 'search', 'value': 'kItChen'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('kitchen', resp.json['data'][0][1].lower())
        params['filters'] = json.dumps([{
            'field': 'comments', 'operator': 'not_search', 'value': 'family'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('family', resp.json['data'][0][1].lower())
        params['filters'] = json.dumps([{
            'field': 'comments',
            'operator': 'in',
            'value': ['Fire Alarm', 'new boiler']}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][1], 'Fire Alarm')

        params['sort'] = json.dumps(['zip', 'occupancytype'])
        params['filters'] = json.dumps([{
            'field': 'occupancytype', 'operator': 'is', 'value': '1-7FAM'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][2], '1-7FAM')
        params['filters'] = json.dumps([{
            'field': 'occupancytype', 'operator': 'is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][2], None)
        params['filters'] = json.dumps([{
            'field': 'occupancytype', 'operator': 'is_not', 'value': '1-7FAM'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(resp.json['data'][0][2], '1-7FAM')
        params['filters'] = json.dumps([{
            'field': 'occupancytype', 'operator': 'not_is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(resp.json['data'][0][2], None)
        # Test nested filters
        params['filters'] = json.dumps([
            ['zip', 'isnot', None],
            {'or': [['zip', '02133'], ['occupancytype', 'Comm']]}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 10)
        self.assertEqual(resp.json['data'][0][0], '02133')
        self.assertEqual(resp.json['data'][9][0], '02134')
        params['filters'] = json.dumps({
            'and': [['zip', '02133'], ['occupancytype', 'Comm']]})
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 4)
        # Group throws an error
        params['group'] = 'zip'
        with six.assertRaisesRegex(self, Exception,
                                   'Group unsupported by this database'):
            resp = self.request(path='/file/%s/database/select' % (
                self.dbFileId, ), user=self.admin, params=params)

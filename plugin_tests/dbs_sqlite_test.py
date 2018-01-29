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

import bz2
import json
import os

from girder import config
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.user import User
from tests import base

# boiler plate to start and stop the server

os.environ['GIRDER_PORT'] = os.environ.get('GIRDER_TEST_PORT', '20200')
config.loadConfig()  # Must reload config to pickup correct port


def setUpModule():
    base.enabledPlugins.append('database_assetstore')
    base.startServer(False)


def tearDownModule():
    base.stopServer()


class DbsSQLiteTest(base.TestCase):
    dbs = {
        'direct': {
            'params': {
                'name': 'Assetstore 1',
                'type': 'database',  # AssetstoreType.DATABASE
            },
            'table': 'albums',
            'columns': ['Title'],  # these columns should exist
        },
        'girder': {
            'params': {
                'name': 'Assetstore 2',
                'type': 'database',  # AssetstoreType.DATABASE
            },
            'table': 'artists',
            'columns': ['Name'],  # these columns should exist
        }
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
        self.admin = User().createUser(**adminUser)
        folders = Folder().childFolders(
            self.admin, 'user', user=self.admin)
        for folder in folders:
            if folder['name'] == 'Public':
                self.publicFolder = folder

        testDBPath = os.path.join(
            os.environ['DATABASE_ASSETSTORE_DATA'], 'chinook_subset.db.bz2')
        testDBData = bz2.BZ2File(testDBPath, 'r').read()
        testDBPath = testDBPath[:-4]
        testDBName = os.path.basename(testDBPath)
        # Save uncompressed for direct tests
        open(testDBPath, 'wb').write(testDBData)
        self.dbs['direct']['params']['dburi'] = 'sqlite:///' + testDBPath
        # Upload to Girder for girder-file tests
        resp = self.request(
            path='/file', method='POST', user=self.admin, params={
                'parentType': 'folder',
                'parentId': self.publicFolder['_id'],
                'name': testDBName,
                'size': len(testDBData)
            })
        self.assertStatusOk(resp)
        fields = [('offset', 0), ('uploadId', resp.json['_id'])]
        files = [('chunk', testDBName, testDBData)]
        resp = self.multipartRequest(
            path='/file/chunk', fields=fields, files=files, user=self.admin)
        self.assertStatusOk(resp)
        self.assertIn('itemId', resp.json)
        self.dbs['girder']['params']['dburi'] = \
            'sqlite:////user/goodlogin/Public/%s/%s' % (testDBName, testDBName)

        for key in self.dbs:
            db = self.dbs[key]
            resp = self.request(
                method='POST', path='/assetstore', user=self.admin,
                params=db['params'])
            self.assertStatusOk(resp)
            db['assetstore'] = resp.json

            params = {
                'parentId': str(self.publicFolder['_id']),
                'parentType': 'folder',
                'table': json.dumps([{
                    'name': db['table'],
                    'database': testDBName,
                    }]),
            }
            resp = self.request(
                path='/database_assetstore/%s/import' % str(
                    db['assetstore']['_id']),
                method='PUT', user=self.admin, params=params)
            self.assertStatusOk(resp)
            db['item'] = list(Item().textSearch(
                db['table'], user=self.admin, limit=1))[0]
            db['file'] = list(Item().childFiles(item=db['item']))[0]
            db['fileId'] = str(db['file']['_id'])

    def testSQLiteDatabaseFields(self):
        # This tests both the Girder and direct databases
        for key in self.dbs:
            db = self.dbs[key]
            resp = self.request(path='/file/%s/database/fields' % (
                db['fileId'], ), user=self.admin)
            self.assertStatus(resp, 200)
            self.assertTrue(len([
                col for col in resp.json if col['name'] == db['columns'][0]]) > 0)

    def testSQLiteDatabaseSelectBasic(self):
        # Only test one database, as we have shown we can already reach the
        # other one
        db = self.dbs['direct']

        # Test the default query
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 50)
        self.assertEqual(resp.json['datacount'], 50)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        # Test limit and offset using a basic sort
        params = {'sort': 'Title', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['datacount'], 5)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        lastData = resp.json
        params['offset'] = 2
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][:-2], lastData['data'][2:])
        # Limit = 0
        params = {'sort': 'Title', 'limit': 0}
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)

    def testSQLiteDatabaseSelectSort(self):
        # Only test one database
        db = self.dbs['girder']
        params = {'sort': 'Name', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        lastData = resp.json
        params = {'sort': 'Name', 'sortdir': -1, 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][:1], lastData['data'][:1])
        self.assertGreater(
            resp.json['data'][0][resp.json['columns']['Name']],
            lastData['data'][0][lastData['columns']['Name']])

    def testSQLiteDatabaseSelectFields(self):
        # Only test one database
        db = self.dbs['direct']
        # Unknown fields aren't allowed
        params = {'fields': 'unknown,Title', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # a comma separated list works
        params['fields'] = 'Title,ArtistId'
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['Title', 'ArtistId'])
        self.assertEqual(resp.json['columns'], {'Title': 0, 'ArtistId': 1})

    def testSQLiteDatabaseSelectFilters(self):
        # Only test one database
        db = self.dbs['direct']
        params = {
            'limit': 5,
            'sort': 'Title',
            'fields': 'Title,AlbumId,ArtistId'
        }
        params['filters'] = json.dumps([])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        params['filters'] = json.dumps([{'field': 'Title', 'value': 'Fireball'}])
        params['limit'] = None
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'Fireball')
        # Test have the value first -- this is allowed in sql
        params['limit'] = 30
        params['filters'] = json.dumps([{
            'lvalue': 'Fireball', 'value': {'field': 'Title'}}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        # test operators
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': '>=', 'value': 'Fireball'}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'Fireball')
        self.assertEqual(resp.json['data'][29][0], 'IV')
        # regex and search
        params['sort'] = json.dumps(['Title'])
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'regex', 'value': 'Live'}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('Live', resp.json['data'][0][0])
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'not_regex', 'value': 'Live'}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('Live', resp.json['data'][0][0])
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'search', 'value': 'liVe'}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('live', resp.json['data'][0][0].lower())
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'not_search', 'value': 'live'}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('live', resp.json['data'][0][0].lower())
        # in
        params['filters'] = json.dumps([{
            'field': 'Title',
            'operator': 'in',
            'value': ['Duos II', 'Blue Moods']}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][0], 'Blue Moods')
        # is
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'is', 'value': True}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'is_not', 'value': True}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        params['filters'] = json.dumps([{
            'field': 'Title', 'operator': 'not_is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        # Test nested filters
        params['filters'] = json.dumps([
            ['AlbumId', 'gte', 40],
            {'or': [['Title', 'Fireball'], ['Title', 'gte', 'Machine']]}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'Fireball')
        self.assertIn('No ', resp.json['data'][29][0])
        params['filters'] = json.dumps({
            'and': [['AlbumId', 'ne', 322],
                    ['Title', 'gte', 'Fireball']]})
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'Fireball')
        self.assertEqual(resp.json['data'][29][0], 'In Step')
        # Test group and a simple function
        params['group'] = json.dumps([{'func': 'substr', 'param': [{'field': 'Title'}, 0, 2]}])
        del params['filters']
        params['fields'] = json.dumps([
            {'func': 'substr', 'param': [{'field': 'Title'}, 0, 2]},
            {'func': 'max', 'param': {'field': 'Title'}},
            {'func': 'max', 'param': {'field': 'AlbumId'}},
            {'func': 'max', 'param': {'field': 'ArtistId'}}])
        resp = self.request(path='/file/%s/database/select' % (
            db['fileId'], ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 27)

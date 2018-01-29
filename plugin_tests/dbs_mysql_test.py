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


class DbsMySqlTest(base.TestCase):
    dbParams = {
        'name': 'Assetstore 1',
        'type': 'database',  # AssetstoreType.DATABASE
        'dburi': 'mysql://anonymous@martdb.ensembl.org:5316/vega_mart_88'
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

        resp = self.request(method='POST', path='/assetstore', user=self.admin,
                            params=self.dbParams)
        self.assertStatusOk(resp)
        self.assetstore1 = resp.json

        params = {
            'parentId': str(self.publicFolder['_id']),
            'parentType': 'folder',
            'table': json.dumps([{
                'name': 'drerio_gene_vega__gene__main',
                'database': 'vega_mart_88',
                }]),
        }
        resp = self.request(
            path='/database_assetstore/%s/import' % str(
                self.assetstore1['_id']),
            method='PUT', user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.dbItem = list(Item().textSearch(
            'drerio_gene_vega__gene__main', user=self.admin, limit=1))[0]
        self.dbFile = list(Item().childFiles(item=self.dbItem))[0]
        self.dbFileId = str(self.dbFile['_id'])

    def testMySqlDatabaseFields(self):
        resp = self.request(path='/file/%s/database/fields' % (
            self.dbFileId, ), user=self.admin)
        self.assertStatus(resp, 200)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'biotype_1020']) > 0)

    def testMySqlDatabaseSelectBasic(self):
        # Test the default query
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 50)
        self.assertEqual(resp.json['datacount'], 50)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        # Test limit and offset using a basic sort
        params = {'sort': 'stable_id_1023', 'limit': 5}
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
        params = {'sort': 'stable_id_1023', 'limit': 0}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)

    def testMySqlDatabaseSelectSort(self):
        params = {'sort': 'stable_id_1023', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        lastData = resp.json
        params = {'sort': 'stable_id_1023', 'sortdir': -1, 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][:1], lastData['data'][:1])
        self.assertGreater(
            resp.json['data'][0][resp.json['columns']['stable_id_1023']],
            lastData['data'][0][lastData['columns']['stable_id_1023']])

    def testMySqlDatabaseSelectFields(self):
        # Unknown fields aren't allowed
        params = {'fields': 'unknown,band_1027', 'limit': 5}
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # a comma separated list works
        params['fields'] = 'stable_id_1023,band_1027'
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['stable_id_1023', 'band_1027'])
        self.assertEqual(resp.json['columns'], {'stable_id_1023': 0, 'band_1027': 1})

    def testMySqlDatabaseSelectFilters(self):
        params = {
            'limit': 5,
            'sort': 'biotype_1020',
            'fields': 'biotype_1020,stable_id_1023,band_1027,ox_ZFIN_xpat_bool'
        }
        params['filters'] = json.dumps([])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        params['filters'] = json.dumps([{'field': 'biotype_1020', 'value': 'IG_pseudogene'}])
        params['limit'] = None
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 26)
        self.assertEqual(resp.json['data'][0][0], 'IG_pseudogene')
        # Test have the value first -- this is allowed in sql
        params['limit'] = 30
        params['filters'] = json.dumps([{
            'lvalue': 'IG_pseudogene', 'value': {'field': 'biotype_1020'}}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 26)
        # test operators
        params['filters'] = json.dumps([{
            'field': 'biotype_1020', 'operator': '>=', 'value': 'IG_pseudogene'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'IG_pseudogene')
        self.assertEqual(resp.json['data'][29][0], 'lincRNA')
        params['sort'] = json.dumps(['biotype_1020', 'stable_id_1023'])
        params['filters'] = json.dumps([{
            'field': 'biotype_1020', 'operator': 'regex', 'value': 'gene'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('IG_gene', resp.json['data'][0][0])
        params['filters'] = json.dumps([{
            'field': 'biotype_1020', 'operator': 'not_regex', 'value': 'gene'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('gene', resp.json['data'][0][0])
        params['filters'] = json.dumps([{
            'field': 'biotype_1020', 'operator': 'search', 'value': 'gEne'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertIn('gene', resp.json['data'][0][0].lower())
        params['filters'] = json.dumps([{
            'field': 'biotype_1020', 'operator': 'not_search', 'value': 'ig_gene'}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotIn('ig_gene', resp.json['data'][0][0].lower())
        params['filters'] = json.dumps([{
            'field': 'biotype_1020',
            'operator': 'in',
            'value': ['IG_pseudogene', 'anitsense']}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][0], 'IG_pseudogene')

        params['filters'] = json.dumps([{
            'field': 'ox_ZFIN_xpat_bool', 'operator': 'is', 'value': True}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][3], 1)
        params['filters'] = json.dumps([{
            'field': 'ox_ZFIN_xpat_bool', 'operator': 'is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'][0][3], None)
        params['filters'] = json.dumps([{
            'field': 'ox_ZFIN_xpat_bool', 'operator': 'is_not', 'value': True}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(resp.json['data'][0][3], 1)
        params['filters'] = json.dumps([{
            'field': 'ox_ZFIN_xpat_bool', 'operator': 'not_is', 'value': None}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(resp.json['data'][0][3], None)
        # Test nested filters
        params['filters'] = json.dumps([
            ['ox_ZFIN_xpat_bool', 'is', None],
            {'or': [['biotype_1020', 'IG_pseudogene'], ['biotype_1020', 'gte', 'linc']]}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'IG_pseudogene')
        self.assertEqual(resp.json['data'][29][0], 'lincRNA')
        params['filters'] = json.dumps({
            'and': [['ox_ZFIN_xpat_bool', 'isnot', None],
                    ['biotype_1020', 'gt', 'lincRNA']]})
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 30)
        self.assertEqual(resp.json['data'][0][0], 'polymorphic_pseudogene')
        self.assertEqual(resp.json['data'][29][0], 'protein_coding')
        # Test group and a simple function
        params['group'] = 'biotype_1020'
        del params['filters']
        params['fields'] = json.dumps([
            'biotype_1020',
            {'func': 'max', 'param': {'field': 'stable_id_1023'}},
            {'func': 'max', 'param': {'field': 'band_1027'}},
            {'func': 'max', 'param': {'field': 'ox_ZFIN_xpat_bool'}}])
        resp = self.request(path='/file/%s/database/select' % (
            self.dbFileId, ), user=self.admin, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 15)

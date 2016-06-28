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
import random
import threading
import time

from girder import config
from tests import base

# boiler plate to start and stop the server

os.environ['GIRDER_PORT'] = os.environ.get('GIRDER_TEST_PORT', '20200')
config.loadConfig()  # Must reload config to pickup correct port


def setUpModule():
    base.enabledPlugins.append('girder_db_items')
    base.startServer(False)


def tearDownModule():
    base.stopServer()


class ItemTest(base.TestCase):
    dbParams = {
        'table': 'towns',
        'type': 'sqlalchemy_postgres',
        'url': os.environ.get('GIRDER_DB_ITEM_DB',
                              'postgresql://postgres@127.0.0.1/sampledb')
    }

    def _setupDbItems(self, args={}):
        """
        Set up db items, one using sqlalchemy_postgres and one sqlalchemy.

        :param args: additional arguments to set on database connections.
        :returns: the two item ids.
        """
        dbParams = self.dbParams.copy()
        dbParams.update(args)
        itemId = str(self.item1['_id'])
        itemId2 = str(self.item2['_id'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(dbParams))
        self.assertStatusOk(resp)
        # Set up item2 with the sqlalchemy connector, so we can test that
        # functions won't work in it.
        dbParams2 = dbParams.copy()
        dbParams2['type'] = 'sqlalchemy'
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId2, ), user=self.admin, type='application/json',
            body=json.dumps(dbParams2))
        self.assertStatusOk(resp)
        return itemId, itemId2

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

    def testItemDatabaseEndpoints(self):
        itemId = str(self.item1['_id'])
        resp = self.request(path='/item/notanitem/database', user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/item/%s/database' % ('f' * len(itemId)),
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])
        resp = self.request(path='/item/%s/database' % itemId, user=self.admin)
        self.assertStatusOk(resp)
        self.assertIsNone(resp.json)
        resp = self.request(path='/item/%s/database' % itemId, user=self.user)
        self.assertStatusOk(resp)
        self.assertIs(resp.json, False)
        # Test the POST endpoint
        resp = self.request(method='POST', path='/item/notanitem/database',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            'f' * len(itemId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps({}))
        self.assertStatus(resp, 400)
        self.assertIn('Unknown database type', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(self.dbParams))
        self.assertStatusOk(resp)
        resp = self.request(path='/item/%s/database' % itemId, user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json, self.dbParams)
        resp = self.request(path='/item/%s/database' % itemId, user=self.user)
        self.assertStatusOk(resp)
        self.assertIs(resp.json, True)
        params = self.dbParams.copy()
        params['url'] = None
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(params))
        self.assertStatus(resp, 400)
        self.assertIn('is invalid', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps({'other': 'value'}))
        params = self.dbParams.copy()
        params['other'] = 'value'
        resp = self.request(path='/item/%s/database' % itemId, user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json, params)
        # Test the delete endpoint
        resp = self.request(method='DELETE', path='/item/notanitem/database',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(method='DELETE', path='/item/%s/database' % (
            'f' * len(itemId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])
        resp = self.request(method='DELETE', path='/item/%s/database' % (
            itemId, ), user=self.user)
        self.assertStatus(resp, 403)
        resp = self.request(method='DELETE', path='/item/%s/database' % (
            itemId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['deleted'], True)
        resp = self.request(method='DELETE', path='/item/%s/database' % (
            itemId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['deleted'], False)

    def testItemDatabaseBadConnectors(self):
        from girder.plugins.girder_db_items import dbs
        self.assertIsNone(dbs.getDBConnector('test1', {'type': 'base'}))
        dbs.base.registerConnectorClass('base', dbs.base.DatabaseConnector)
        self.assertIsNone(dbs.getDBConnector('test1', {'type': 'base'}))
        del dbs.base._connectorClasses['base']

    def testItemDatabaseBaseConnectorClass(self):
        from girder.plugins.girder_db_items import dbs
        conn = dbs.base.DatabaseConnector()
        res = conn.performSelect()
        self.assertEqual(res['data'], [])
        self.assertEqual(res['fields'], [])
        self.assertFalse(conn.validate())
        self.assertTrue(conn.checkOperatorDatatype('unknown', 'unknown'))

    def testItemDatabaseFields(self):
        itemId = str(self.item1['_id'])
        resp = self.request(path='/item/%s/database/fields' % (
            itemId, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(self.dbParams))
        self.assertStatusOk(resp)
        resp = self.request(path='/item/notanitem/database/fields',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/item/%s/database/fields' % (
            'f' * len(itemId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])
        resp = self.request(path='/item/%s/database/fields' % (
            itemId, ), user=self.admin)
        self.assertStatusOk(resp)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'town']) > 0)
        resp = self.request(path='/item/%s/database/fields' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertTrue(len([
            col for col in resp.json if col['name'] == 'town']) > 0)
        # break the database link
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps({'url': self.dbParams['url'] + '_notpresent'}))
        self.assertStatusOk(resp)
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/fields' % (
                itemId, ), user=self.admin)
        # break the information in the item to make sure that we fail as
        # expected
        from girder.plugins.girder_db_items import dbs, dbInfoKey
        item = self.model('item').load(id=itemId, force=True)
        item[dbInfoKey]['url'] = ''
        self.model('item').save(item)
        dbs.base._connectorCache.pop(str(item['_id']), None)
        resp = self.request(path='/item/%s/database/fields' % (
            itemId, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Failed to connect', resp.json['message'])

    def testItemDatabaseRefresh(self):
        itemId = str(self.item1['_id'])
        resp = self.request(method='PUT', path='/item/%s/database/refresh' % (
            itemId, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(self.dbParams))
        self.assertStatusOk(resp)
        resp = self.request(
            method='PUT', path='/item/notanitem/database/refresh',
            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(method='PUT', path='/item/%s/database/refresh' % (
            'f' * len(itemId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])
        resp = self.request(method='PUT', path='/item/%s/database/refresh' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], False)
        # Get fields so we will have something to refresh
        resp = self.request(path='/item/%s/database/fields' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        resp = self.request(method='PUT', path='/item/%s/database/refresh' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], True)
        resp = self.request(method='PUT', path='/item/%s/database/refresh' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['refreshed'], False)

    def testItemDatabaseView(self):
        # Test that we can get data from a view (this is the same as accessing
        # a table without a primary key)
        itemId = str(self.item1['_id'])
        params = self.dbParams.copy()
        params['table'] = 'geometry_columns'
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(params))
        self.assertStatusOk(resp)
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertGreater(len(resp.json['data']), 10)
        self.assertGreater(resp.json['datacount'], 10)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))

    def testItemDatabaseSelectBasic(self):
        itemId = str(self.item1['_id'])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('not a database link', resp.json['message'])
        resp = self.request(path='/item/notanitem/database/select',
                            user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid ObjectId', resp.json['message'])
        resp = self.request(path='/item/%s/database/select' % (
            'f' * len(itemId)), user=self.admin)
        self.assertStatus(resp, 400)
        self.assertIn('Invalid item', resp.json['message'])

        itemId, itemId2 = self._setupDbItems()
        # Test the default query
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 50)
        self.assertEqual(resp.json['datacount'], 50)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        # Test limit and offset using a basic sort
        params = {'sort': 'town', 'limit': 5}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['datacount'], 5)
        self.assertEqual(len(resp.json['columns']), len(resp.json['fields']))
        lastData = resp.json
        params['offset'] = 2
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][:-2], lastData['data'][2:])
        # break the database link
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps({'url': self.dbParams['url'] + '_notpresent'}))
        self.assertStatusOk(resp)
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId, ), user=self.admin, params=params)
        # break the information in the item to make sure that we fail as
        # expected
        from girder.plugins.girder_db_items import dbs, dbInfoKey
        item = self.model('item').load(id=itemId, force=True)
        item[dbInfoKey]['url'] = ''
        self.model('item').save(item)
        dbs.base._connectorCache.pop(str(item['_id']), None)
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.admin, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Failed to connect', resp.json['message'])

    def testItemDatabaseSelectSort(self):
        itemId, itemId2 = self._setupDbItems()
        # Test a variety of sorts
        params = {'sort': 'town', 'limit': 5}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        lastData = resp.json
        params = {'sort': 'town', 'sortdir': -1, 'limit': 5}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][:1], lastData['data'][:1])
        self.assertGreater(resp.json['data'][0][resp.json['columns']['town']],
                           lastData['data'][0][lastData['columns']['town']])
        # Use a json sort specification
        params = {'sort': json.dumps(['town']), 'limit': 5}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], lastData['data'])
        # This should work fine on item2
        resp = self.request(path='/item/%s/database/select' % (
            itemId2, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], lastData['data'])
        # Use a function
        params['sort'] = json.dumps([{
            'func': 'mod', 'param': [{'field': 'pop2010'}, 10]},
            ['town', -1]
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(
            int(resp.json['data'][0][resp.json['columns']['pop2010']]) % 10, 0)
        self.assertGreater(resp.json['data'][0][resp.json['columns']['town']],
                           resp.json['data'][1][resp.json['columns']['town']])
        # This must not work on item2
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId2, ), user=self.user, params=params)
        # Test with bad parameters
        params['sort'] = '["not valid json'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['sort'] = json.dumps({'not': ['a', 'list']})
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['sort'] = 'unknownfield'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        params['sort'] = json.dumps([['town'], ['unknownfield', -1]])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])

    def testItemDatabaseSelectFields(self):
        itemId, itemId2 = self._setupDbItems()
        # Unknown fields aren't allowed
        params = {'fields': 'unknown,town', 'limit': 5}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # a comma separated list works
        params['fields'] = 'town,pop2010'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # extra commas and white space at the ends of field names are allowed
        params['fields'] = 'town ,, pop2010 ,,'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # You can use json instead
        params['fields'] = json.dumps(['town', 'pop2010'])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], ['town', 'pop2010'])
        self.assertEqual(resp.json['columns'], {'town': 0, 'pop2010': 1})
        # Invalid json fails
        params['fields'] = '["not valid json",town'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        # instead of a field name, you can use a function
        params['fields'] = json.dumps([
            'town',
            {'func': 'mod', 'param': [{'field': 'pop2010'}, 10]},
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'town': 0, 'column_1': 1})
        # This must not work on item2
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId2, ), user=self.user, params=params)
        # We can use a reference to better find our column
        params['fields'] = json.dumps([
            'town',
            {
                'func': 'mod',
                'param': [{'field': 'pop2010'}, 10],
                'reference': 'popmod'
            },
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], json.loads(params['fields']))
        self.assertEqual(resp.json['columns'], {'town': 0, 'popmod': 1})
        # Test some function handling
        params['sort'] = 'town'
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'field': 'town'}}
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'abington')
        # This uses 'town' as a value in the first field, not a field
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': 'town'},
            'town'
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'town')
        # Function parameters must be fields, values, or otehr functions
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'unknown': 'town'}}
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # Fields in functions must exist
        params['fields'] = json.dumps([
            {'func': 'lower', 'param': {'field': 'unknown'}}
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])
        # We don't have to use a function
        params['fields'] = json.dumps([{'field': 'town'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'ABINGTON')
        # But it needs to be a field or a function
        params['fields'] = json.dumps([{'unknown': 'town'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must use known fields', resp.json['message'])

    def testItemDatabaseSelectFilterViaParams(self):
        itemId, itemId2 = self._setupDbItems()
        # We can access filters either via the filter parameter or via the name
        # of each field optionally suffixed with different operators.
        baseParams = {'limit': 5, 'sort': 'town', 'fields': 'town'}
        # Exact match
        params = dict(baseParams.items() + {'town': 'BOSTON'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params = dict(baseParams.items() + {'town': 'boston'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 0)
        # minimum
        params = dict(baseParams.items() + {'town_min': 'BOS'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # search
        params = dict(baseParams.items() + {'town_search': '^bo.*n$'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 3)
        self.assertEqual(resp.json['data'][1][0], 'BOSTON')
        # compound
        params = dict(baseParams.items() + {
            'town_min': 'BOS',
            'town_notsearch': '^bo.*n$'
        }.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertNotEqual(resp.json['data'][0][0], 'BOSTON')
        # numeric comparisons are sent as text
        params = dict(baseParams.items() + {'pop2010_min': '150000'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 3)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # you can't use regex or search on numeric types
        params = dict(baseParams.items() + {'pop2010_search': '150000'}.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Cannot use search operator on field',
                      resp.json['message'])
        # We should be able to get the same results regardless of whether we
        # use not or not_
        params = dict(baseParams.items() + {
            'town_min': 'BOS',
            'town_notin': 'BOSTON'
        }.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOURNE')
        params = dict(baseParams.items() + {
            'town_min': 'BOS',
            'town_not_in': 'BOSTON'
        }.items())
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOURNE')

    def testItemDatabaseSelectFilters(self):
        itemId, itemId2 = self._setupDbItems()
        params = {'limit': 5, 'sort': 'town', 'fields': 'town'}
        params['filters'] = '[not json'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['filters'] = json.dumps({'town': 'BOSTON'})
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must be a JSON list', resp.json['message'])
        params['filters'] = json.dumps([{'town': 'BOSTON'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must specify a field or func', resp.json['message'])
        params['filters'] = json.dumps([{'field': 'town', 'value': 'BOSTON'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # Test have the value first
        params['filters'] = json.dumps([{
            'lvalue': 'BOSTON', 'value': {'field': 'town'}}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # test operators
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': '>=', 'value': 'BOS'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'gt', 'value': 'BOS'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([{
            'field': 'town', 'operator': 'noop', 'value': 'BOS'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Unknown filter operator', resp.json['message'])
        # Functions must be known
        params['filters'] = json.dumps([{
            'field': 'town', 'value': {'func': 'unknown', 'params': []}
        }])
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId, ), user=self.user, params=params)
        # We throw a different error when params is an empty dict
        params['filters'] = json.dumps([{
            'field': 'town', 'value': {'func': 'unknown', 'param': {}}}])
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId, ), user=self.user, params=params)
        # Test a filter composed of a list
        params['filters'] = json.dumps([['town', 'gt', 'BOS']])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 5)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'BOSTON']])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        params['filters'] = json.dumps([['town', 'gt', 'BOSTON', 'extra']])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have two or three components',
                      resp.json['message'])
        # Fail on an unknown field
        params['filters'] = json.dumps([['unknown', 'BOSTON']])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('Filters must be on known fields', resp.json['message'])
        # Fail without a value
        params['filters'] = json.dumps([{
            'field': 'town'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatus(resp, 400)
        self.assertIn('must have a value or rfunc', resp.json['message'])
        # Test a right function
        params['filters'] = json.dumps([{
            'field': 'town', 'rfunc': 'upper', 'rparam': 'boston'}])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'BOSTON')
        # This must not work on item2
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId2, ), user=self.user, params=params)
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
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(len(resp.json['data']), 1)
        self.assertEqual(resp.json['data'][0][0], 'RUTLAND')

    def testItemDatabaseSelectDictFormat(self):
        itemId, itemId2 = self._setupDbItems()
        params = {'sort': 'town', 'limit': 5, 'format': 'dict'}
        params['fields'] = 'town,pop2010,shape_len,type'
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['fields'], [
            'town', 'pop2010', 'shape_len', 'type'])
        self.assertEqual(resp.json['columns'], {
            'town': 0, 'pop2010': 1, 'shape_len': 2, 'type': 3})
        self.assertTrue(isinstance(resp.json['data'][0], dict))
        self.assertEqual(set(resp.json['data'][0].keys()),
                         set(['town', 'pop2010', 'shape_len', 'type']))

    def testItemDatabaseSelectClient(self):
        itemId, itemId2 = self._setupDbItems()
        params = {'sort': 'town', 'limit': 1, 'clientid': 'test'}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        from girder.plugins.girder_db_items import dbs
        sessions = dbs.base._connectorCache[itemId].sessions
        # We should be tracking the a session for 'test'
        self.assertIn('test', sessions)
        self.assertFalse(sessions['test']['used'])
        last = sessions['test'].copy()
        # A new request should update the last used time
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertGreater(sessions['test']['last'], last['last'])
        self.assertEqual(sessions['test']['session'], last['session'])
        # Artifically age the last session and test that we get a new session
        last = sessions['test'].copy()
        sessions['test']['last'] -= 305  # 300 is the default expiry age
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertNotEqual(sessions['test']['session'], last['session'])
        # Send a slow query in a thread.  Use a random number as part of the
        # query to prevent caching of the results.
        slowParams = params.copy()
        slowParams['fields'] = json.dumps([
            'town',
            {'func': 'st_hausdorffdistance', 'param': [
                {'func': 'st_minimumboundingcircle', 'param': {
                    'field': 'geom'}},
                {'field': 'geom'},
                0.03 + 0.01 * random.random()]},
        ])
        slowParams['limit'] = 500
        slowResults = {}

        def slowQuery(params):
            try:
                self.request(path='/item/%s/database/select' % (
                    itemId, ), user=self.user, params=slowParams)
            except Exception as exc:
                slowResults['exc'] = exc

        slow = threading.Thread(target=slowQuery, kwargs={
            'params': params
        })
        slow.start()
        # Wait for the query to start
        while not sessions['test']['used'] and slow.is_alive():
            time.sleep(0.05)
        # Sending a normal request should cancel the slow one and respond
        # promptly
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        # The slow request should be cancelled
        slow.join()
        self.assertTrue(
            'canceling statement due to user' in slowResults['exc'].message or
            'InterruptedException' in slowResults['exc'].message)

    def testItemDatabaseSelectPolling(self):
        # Create a test database connector so we can check polling
        from girder.plugins.girder_db_items import dbs

        dbInfo = {'queries': 0, 'data': [[1]]}

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
                return results

            @staticmethod
            def validate(*args, **kwargs):
                return True

        dbs.base.registerConnectorClass(TestConnector.name, TestConnector)
        itemId, itemId2 = self._setupDbItems({'type': 'test'})
        params = {}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        params = {'wait': 1}
        # Waiting shouldn't affect the results since there is data available
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['data'], [[1]])
        # If no data is available for the wait duration, we can get a null
        # response
        dbInfo['data'].pop()
        lastCount = dbInfo['queries']
        params = {'wait': 0.01, 'poll': 0.01}
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
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
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        # Don't depend on exact counts, as the test could be slow
        self.assertEqual(resp.json['data'], [[2]])
        self.assertGreater(dbInfo['queries'], lastCount + 3)
        self.assertLess(dbInfo['queries'], lastCount + 9)
        add.join()

        # Test if we have bad data we get an exception
        dbInfo['data'] = None
        with self.assertRaises(Exception):
            resp = self.request(path='/item/%s/database/select' % (
                itemId, ), user=self.user, params=params)

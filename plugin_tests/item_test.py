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

    def _setupDbItems(self):
        """
        Set up two db items, one using sqlalchemy_postgres and one sqlalchemy.

        :returns: the two item ids.
        """
        itemId = str(self.item1['_id'])
        itemId2 = str(self.item2['_id'])
        resp = self.request(method='POST', path='/item/%s/database' % (
            itemId, ), user=self.admin, type='application/json',
            body=json.dumps(self.dbParams))
        self.assertStatusOk(resp)
        # Also set up item2 with the sqlalchemy connector, so we can test that
        # functions won't work in it.
        dbParams2 = self.dbParams.copy()
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
        self.admin, self.user =\
            [self.model('user').createUser(**user) for user in users]
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
            'town'
        ])
        resp = self.request(path='/item/%s/database/select' % (
            itemId, ), user=self.user, params=params)
        self.assertStatusOk(resp)
        self.assertEqual(
            int(resp.json['data'][0][resp.json['columns']['pop2010']]) % 10, 0)
        self.assertLess(resp.json['data'][0][resp.json['columns']['town']],
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

    # test fields, fields with functions

    # filter, filter via params, filter with functions, operators per datatypes

    # test polling (possibly with subclasses connector so we don't need an
    # actual database)

    # test client and cancelling queries

    # test subclassing connector to try test base connector defaults

    # Add code for returning dictionary format

    # Test with a view instead of a table

    # Debug to be removed
    # import sys
    # sys.stderr.write('Resp: %r\n' % resp.json)
    # self.assertTrue(False)

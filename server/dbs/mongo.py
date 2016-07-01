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

from pymongo import MongoClient

from . import base

from girder.api.rest import RestException


def inferFields(records):
    fields = set()
    for r in records:
        for field in r:
            fields.add(field)
    return list(fields)


def convertFields(headers, row):
    return map(lambda field: row.get(field), headers)


class MongoConnector(base.DatabaseConnector):
    name = 'mongo'

    def __init__(self, *args, **kwargs):
        super(MongoConnector, self).__init__(*args, **kwargs)
        if not self.validate(**kwargs):
            return

        self.databaseName = kwargs.get('database')
        self.collection = kwargs.get('collection')
        self.databaseUrl = kwargs.get('url')

        self.fieldInfo = None

        self.initialized = True

    def _applyFilter(self, clauses, filt):
        operator = filt['operator']
        operator = base.FilterOperators.get(operator)

        if operator in ['eq', 'ne', 'lt', 'gte']:
            field = filt['field']
            value = filt['value']
            operator = '$' + operator

            clauses.append({field: {operator: value}})
        else:
            raise RestException('operator %s unimplemented' % (operator))

        return clauses

    def connect(self):
        self.conn = MongoClient(self.databaseUrl)
        if self.databaseName:
            self.database = self.conn[self.database]
        else:
            self.database = self.conn.get_default_database()
        return self.database[self.collection]

    def disconnect(self):
        self.conn.close()
        self.conn = None

    def performSelect(self, fields, queryProps={}, filters=[], client=None):
        coll = self.connect()

        result = super(MongoConnector, self).performSelect(
            fields, queryProps, filters)

        filterQueryClauses = []
        for filt in filters:
            filterQueryClauses = self._applyFilter(filterQueryClauses, filt)

        opts = {}
        for k, v in queryProps.iteritems():
            target = None
            if k == 'fields':
                target = 'projection'
                if v == []:
                    v = None
                else:
                    v = {field: True for field in v}
                if '_id' not in v:
                    v['_id'] = False
            elif k == 'offset':
                target = 'skip'
            elif k in ['limit', 'no_cursor_timeout', 'cursor_type', 'sort',
                       'allow_partial_results', 'oplog_replay', 'modifiers']:
                target = k

            if target is not None:
                opts[target] = v

        if len(filterQueryClauses) > 0:
            opts['filter'] = {'$and': filterQueryClauses}
        elif 'filter' in opts:
            del opts['filter']

        results = coll.find(**opts)
        results = [convertFields(result['fields'], row) for row in results]

        result['data'] = results

        self.disconnect()

        return result

    def getFieldInfo(self):
        if self.fieldInfo is None:
            # cache the fieldInfo so we don't process all of the documents
            # every time.
            coll = self.connect()

            fields = {}
            for result in coll.find():
                fields.update(result)
            headers = inferFields([fields])

            fieldInfo = []
            for h in sorted(headers):
                fieldInfo.append({'name': h,
                                  'type': 'unknown'})
            self.fieldInfo = fieldInfo
        return self.fieldInfo

    @staticmethod
    def getTableList(url, **kwargs):
        """
        Get a list of known collections from the database.

        :param url: url to connect to the database.
        :returns: A list of known collections.
        """
        conn = MongoClient(url)
        database = conn.get_default_database()
        collections = database.collection_names(False)
        return collections

    @staticmethod
    def validate(url=None, database=None, collection=None, **kwargs):
        return url and collection


base.registerConnectorClass(MongoConnector.name, MongoConnector, {
    'dialects': {
        'mongodb': 'mongodb',
        'mongo': 'mongodb',
    },
    'default_dialect': 'mongodb',
    'priority': 0,
})

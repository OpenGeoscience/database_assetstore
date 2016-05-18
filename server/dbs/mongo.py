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

        self.database = kwargs.get('database')
        self.collection = kwargs.get('collection')
        self.databaseUrl = kwargs.get('url')

        self.initialized = True

    def _applyFilter(self, clauses, filt):
        operator = filt['operator']
        operator = base.FilterOperators.get(operator)

        if operator in ['eq', 'ne']:
            field = filt['field']
            value = filt['value']
            operator = '$' + operator

            clauses.append({field: {operator: value}})
        else:
            raise RestException('operator %s unimplemented' % (operator))

        return clauses

    def connect(self):
        self.conn = MongoClient(self.databaseUrl)

    def disconnect(self):
        self.conn.close()
        self.conn = None

    def performSelect(self, fields, queryProps={}, filters=[], client=None):
        self.connect()

        result = super(MongoConnector, self).performSelect(fields, queryProps, filters)
        coll = self.conn[self.database][self.collection]

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
            elif k == 'offset':
                target = 'skip'
            elif k in ['limit', 'no_cursor_timeout', 'cursor_type', 'sort', 'allow_partial_results', 'oplog_replay', 'modifiers']:
                target = k

            if target is not None:
                opts[target] = v

        if len(filterQueryClauses) > 0:
            opts['filter'] = {'$and': filterQueryClauses}
        elif 'filter' in opts:
            del opts['filter']

        results = list(coll.find(**opts))
        headers = inferFields(results)
        results = [convertFields(headers, row) for row in results]

        result['data'] = results

        self.disconnect()

        return result

    def getFieldInfo(self):
        self.connect()
        coll = self.conn[self.database][self.collection]

        results = coll.find()
        headers = inferFields(results)

        fieldInfo = []
        for h in headers:
            fieldInfo.append({'name': h,
                              'type': 'unknown'})

        return fieldInfo

    @staticmethod
    def validate(url=None, database=None, collection=None, **kwargs):
        return url and database and collection


base.registerConnectorClass(MongoConnector.name, MongoConnector)

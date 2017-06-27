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

import bson.json_util
import re
import six
from pymongo import MongoClient

from girder import logger as log

from . import base
from .base import DatabaseConnectorException


MongoOperators = {
    'eq': '$eq',
    'ne': '$ne',
    'gt': '$gt',
    'gte': '$gte',
    'lt': '$lt',
    'lte': '$lte',
    'in': '$in',
    'not_in': '$nin',
    'regex': '$regex',
    # not_regex, search, and not_search are handled as special cases
    # search is treated as a case-insensitive, multiline regex
    # is and not_is are the same as $eq and $ne unless the value is None
}


class MongoConnector(base.DatabaseConnector):
    name = 'mongo'
    databaseNameRequired = False

    def __init__(self, *args, **kwargs):
        super(MongoConnector, self).__init__(*args, **kwargs)
        self.collection = kwargs.get('collection', kwargs.get('table'))
        self.databaseUrl = kwargs.get('url')
        self.databaseName = kwargs.get(
            'database', base.databaseFromUri(self.databaseUrl))

        self.fieldInfo = None

        self.initialized = True

    def _addFilter(self, clauses, filter):
        """
        Add a filter to a list of clauses.

        :param clauses: a list which is modified.
        :param filter: the filter to add.  This needs to be a dictionary with
            field, operator, and value or with group and value.
        :return: the list of clauses.
        """
        if 'group' in filter:
            subclauses = []
            for subfilter in filter['value']:
                subclauses = self._addFilter(subclauses, subfilter)
            clauses.append({'$' + filter['group']: subclauses})
            return clauses
        operator = filter['operator']
        operator = base.FilterOperators.get(operator)

        field = filter['field']
        if not isinstance(field, six.string_types):
            raise DatabaseConnectorException(
                'Filters must use a known field as the left value')
        value = filter['value']
        if operator in MongoOperators:
            operator = MongoOperators[operator]
        elif operator == 'not_regex':
            operator = '$not'
            value = re.compile(filter['value'])
        elif operator in ('search', 'not_search'):
            operator = '$regex' if operator == 'search' else '$not'
            value = re.compile(filter['value'],
                               re.IGNORECASE | re.MULTILINE | re.DOTALL)
        elif operator in ('is', 'not_is'):
            if value is None:
                operator = '$in' if operator == 'is' else '$nin'
                value = [None]
            else:
                operator = '$eq' if operator == 'is' else '$ne'
        else:
            raise DatabaseConnectorException('operator %s unimplemented' % (
                operator))
        clauses.append({field: {operator: value}})
        return clauses

    def connect(self):
        """
        Connect to the database and get a reference to the Mongo collection.

        :returns: the mongo collection.
        """
        self.conn = MongoClient(self.databaseUrl)
        self.database = self.conn[self.databaseName]
        return self.database[self.collection]

    def disconnect(self):
        """
        Disconnect from the database.
        """
        self.conn.close()
        self.conn = None

    def performSelect(self, fields, queryProps={}, filters=[], client=None):
        """
        Select data from the database.  The results are passed back as a
        dictionary with the following values:
          limit: the limit used in the query
          offset: the offset used in the query
          sort: the list of sort parameters used in the query.
          fields: a list of the fields that are being returned in the order
        that they are returned.
          data: a list with one entry per row of results.  Each entry is a list
        with one entry per column.

        :param fields: the results from getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           and sort.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See above.
        """
        result = super(MongoConnector, self).performSelect(
            fields, queryProps, filters)

        if queryProps.get('group'):
            raise DatabaseConnectorException(
                'Group unsupported by this database.')

        filterQueryClauses = []
        for filt in filters:
            filterQueryClauses = self._addFilter(filterQueryClauses, filt)

        opts = {}
        for k, v in six.iteritems(queryProps):
            target = None
            if k == 'fields' and v and v != []:
                target = 'projection'
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

        result['format'] = 'dict'
        if queryProps.get('limit') == 0:
            result['data'] = []
        else:
            if queryProps.get('limit') < 0:
                opts['limit'] = 0
            coll = self.connect()
            log.info('Query: %s', bson.json_util.dumps(
                opts, check_circular=False, separators=(',', ':'),
                sort_keys=False, default=str, indent=None))
            cursor = coll.find(**opts)
            result['datacount'] = cursor.count(True)
            result['data'] = cursor
            self.disconnect()

        return result

    def getFieldInfo(self):
        """
        Return a list of fields that are known and can be queried.

        :return: a list of known fields.  Each entry is a dictionary with name,
                 datatype, and optionally a description.
        """
        if self.fieldInfo is None:
            # cache the fieldInfo so we don't process all of the documents
            # every time.
            # TODO: either have a maximum duration or some other method of
            # analyzing a subset of the table; on a large table this takes a
            # long time.
            coll = self.connect()

            fields = {}
            for result in coll.find():
                fields.update(result)

            fieldInfo = []
            for field in sorted(six.iterkeys(fields)):
                fieldInfo.append({'name': field,
                                  'type': 'unknown'})
            self.fieldInfo = fieldInfo
        return self.fieldInfo

    @staticmethod
    def getTableList(url, internalTables=False, **kwargs):
        """
        Get a list of known databases, each of which has a list of known
        collections from the database.  This is of the form [{'database':
        (database 1), 'tables': [{'table': (collection 1)}, {'table':
        (collection 2)}, ...]}, {'database': (database 2), 'tables': [...]},
        ...]

        :param url: url to connect to the database.
        :returns: A list of known tables.

        :param url: url to connect to the database.
        :param internaltables: True to return tables about the database itself.
            Ignored for Mongo.
        :returns: A list of known collections.
        """
        conn = MongoClient(url)
        databaseName = base.databaseFromUri(url)
        if databaseName is None:
            databaseNames = conn.database_names()
        else:
            databaseNames = [databaseName]
        results = []
        for name in databaseNames:
            database = conn[name]
            results.append({
                'database': name,
                'tables': [{'table': collection, 'name': collection}
                           for collection in database.collection_names(False)]
            })
        return results

    @staticmethod
    def validate(url=None, database=None, collection=None, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        return url and collection

    @staticmethod
    def jsonDumps(*args, **kwargs):
        """
        Use the bson utility to dump JSON.  This handles special BSON
        datatypes.  See json.dumps for the function paramters.
        """
        return bson.json_util.dumps(*args, **kwargs)


base.registerConnectorClass(MongoConnector.name, MongoConnector, {
    'dialects': {
        'mongodb': 'mongodb',
        'mongo': 'mongodb',
    },
    'default_dialect': 'mongodb',
    'priority': 0,
})

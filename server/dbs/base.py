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

import time

_connectorClasses = {}
_connectorCache = {}


FilterOperators = {
    'eq': 'eq',
    None: 'eq',
    '=': 'eq',
    'ne': 'ne',
    '!=': 'ne',
    '<>': 'ne',
    'gte': 'gte',
    'min': 'gte',
    '>=': 'gte',
    'gt': 'gt',
    '>': 'gt',
    'lt': 'lt',
    'max': 'lt',
    '<': 'lt',
    'lte': 'lte',
    '<=': 'lte',
    'in': 'in',
    'notin': 'notin',
    'regex': 'regex',
    '~': 'regex',
    'search': 'search',  # case insensitive regex
    '~*': 'search',
    'notregex': 'notregex',
    '!~': 'notregex',
    'notsearch': 'notsearch',
    '!~*': 'notsearch',
}
DatatypeOperators = {
    'array': {'in', 'notin'},
    'boolean': {'eq', 'ne', 'in', 'notin'},
    'date': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'notin'},
    'duration': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'notin'},
    'enum': {'eq', 'ne', 'in', 'notin'},
    'number': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'notin'},
    'string': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'notin', 'regex',
               'notregex', 'search', 'notsearch'},
}


def getDBConnectorClass(name):
    """
    Get a DB connector class.  This checks if such a class exists and either
    returns a reference to the class or None.

    :param name: name of the connector class, as registered by __init__.
    :return: the connector class or None
    """
    return _connectorClasses.get(name)


def registerConnectorClass(name, cls):
    """
    Register a connector class with a specific name.

    :param name: the name to register.  This is what an item has to specify in
                 the 'type' field.
    :param cls: a reference to the connector class.
    """
    _connectorClasses[name] = cls


def clearDBConnectorCache(id):
    """
    If the id has been used in the connector cache, discard the entry.

    :param id: key for the connector cache.
    """
    id = str(id)
    if id in _connectorCache:
        del _connectorCache[id]


def getDBConnector(id, dbinfo):
    """
    Get a specific DB connector, caching it if possible.

    :param id: key for the connector cache.
    :param dbinfo: a dictionary of information to pass to the connector
    :return: the connector instance, or None if none available.
    """
    id = str(id)
    if id not in _connectorCache:
        connClass = getDBConnectorClass(dbinfo.get('type'))
        if connClass is None:
            return None
        conn = connClass(**dbinfo)
        if conn is None:
            return None
        if not getattr(conn, 'initialized', None):
            return None
        _connectorCache[id] = conn
    return _connectorCache[id]


class DatabaseConnector(object):
    def __init__(self, *args, **kwargs):
        self.initialized = False

    def checkOperatorDatatype(self, field, operator, fieldList=None):
        """
        Check if the specified operator is allowed on a specific field,
        probably based on the field type.

        :param field: the name of the field that will be operated on.
        :param operator: the operator to check.
        :param fieldList: the known field list, probably as returned by
                          getFieldInfo.
        :returns: True if the operator is allowed.
        """
        datatype = None
        if fieldList is None:
            fieldList = self.getFieldInfo()
        for field in fieldList:
            if field == field.get('name'):
                datatype = field.get('datatype')
                break
        if datatype in DatatypeOperators:
            return operator in DatatypeOperators[datatype]
        return True

    def getFieldInfo(self):
        """
        Return a list of fields that are known and can be queried.

        :return: a list of known fields.  Each entry is a dictionary with name,
                 datatype, and optionally a description.
        """
        return []

    def isField(self, name, fields=None):
        """
        Check if a specified name is a valid field.  If so, return the
        canonical field name.

        :param name: the name to check.
        :param fields: the results from getFieldInfo.  If None, this calls
                       getFieldInfo.
        :return: False if this is not a known field, otherwise the cannonical
                 field name.
        """
        if fields is None:
            fields = self.getFieldInfo()
        for field in fields:
            if name == field.get('name'):
                return True
        return False

    def performSelect(self, fields=None, queryProps={}, filters=[],
                      client=None, queryInfo=None):
        """
        Perform a select query.  The results are passed back as a dictionary
        with the following values:
          limit: the limit used in the query
          offset: the offset used in the query
          sort: the list of sort parameters used in the query.
          fields: a list of the fields that are being returned in the order
        that they are returned.
          data: a list with one entry per row of results.  Each entry is a list
        with one entry per column.

        :param fields: the results from getFieldInfo.  If None, this may call
                       getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           sort, fields, wait, poll, and initwait.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See above.
        """
        if queryProps.get('fields') is None:
            if not fields:
                fields = self.getFieldInfo()
            queryProps['fields'] = [field['name'] for field in fields]
        return {
            'limit': queryProps.get('limit'),
            'offset': queryProps.get('offset'),
            'sort': queryProps.get('sort'),
            'fields': queryProps.get('fields'),
            'data': []
        }

    def performSelectWithPolling(self, fields=None, queryProps={},
                                 *args, **kwargs):
        """
        Perform a select query.  If polling is enabled, wait the initial wait
        before making the query, then perform the query at the polling interval
        until either at least one data item has been returned or the wait time
        has elapsed.  See performSelect for more information.

        :param fields: the results from getFieldInfo.  If None, this may call
                       getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           sort, fields, wait, poll, and initwait.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See performSelect.
        """
        wait = queryProps.get('wait')
        if not wait:
            return self.performSelect(fields, queryProps, *args, **kwargs)
        if queryProps.get('initwait'):
            time.sleep(queryProps.get('initwait'))
        poll = queryProps.get('poll', 10)

        starttime = time.time()
        result = self.performSelect(fields, queryProps, *args, **kwargs)
        while result is not None and not len(result['data']):
            curtime = time.time()
            if curtime >= starttime + wait:
                break
            # We wait the poll interval unless less than that amount of time is
            # left in our wait cycle.  If that is the case, we wait the
            # greater of the remaining time and half the poll interval.  This
            # means that the total wait time can be up to half the poll
            # internval plus the query time longer than that specified.
            time.sleep(max(min(poll, starttime + wait - curtime), poll * 0.5))
            result = self.performSelect(fields, queryProps, *args, **kwargs)
        return result

    def prepareSelect(self, fields=None, queryProps={}, filters=[],
                      client=None):
        """
        Prepare to perform a select query.

        :param fields: the results from getFieldInfo.  If None, this may call
                       getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           sort, fields, wait, poll, and initwait.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: an object with query information that can be used by
                 performSelect or performSelectWithPolling.
        """
        return None

    @staticmethod
    def validate(*args, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        return False

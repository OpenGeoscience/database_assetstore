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
    'not_in': 'not_in',
    'notin': 'not_in',
    'regex': 'regex',
    '~': 'regex',
    'search': 'search',  # case insensitive regex
    '~*': 'search',
    'not_regex': 'not_regex',
    'notregex': 'not_regex',
    '!~': 'not_regex',
    'notsearch': 'not_search',
    'notsearch': 'not_search',
    '!~*': 'not_search',
    'is': 'is',
    'not_is': 'not_is',
    'notis': 'not_is',
    'isnot': 'not_is',
}
DatatypeOperators = {
    'array': {'in', 'not_in'},
    'boolean': {'eq', 'ne', 'in', 'not_in'},
    'date': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'duration': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'enum': {'eq', 'ne', 'in', 'not_in'},
    'number': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'string': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'regex',
               'not_regex', 'search', 'not_search'},
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
        return True
    return False


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
        if not getattr(conn, 'initialized', None):
            return None
        _connectorCache[id] = conn
    return _connectorCache[id]


class DatabaseConnectorException(Exception):
    pass


class DatabaseConnector(object):
    def __init__(self, *args, **kwargs):
        self.initialized = False
        self.allowFieldFunctions = False
        self.allowSortFunctions = False
        self.allowFilterFunctions = False

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
        for fieldEntry in fieldList:
            if field == fieldEntry.get('name'):
                datatype = fieldEntry.get('datatype', fieldEntry.get('type'))
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

    def isField(self, name, fields=None, allowFunc=False):
        """
        Check if a specified name is a valid field.  If so, return the
        canonical field name or True if a function.

        :param name: the name to check.  This can also be a dictionary with
                     'field' or ('func' and optionally 'param').
        :param fields: the results from getFieldInfo.  If None, this calls
                       getFieldInfo.
        :param allowFunc: if True, also allow left functions.
        :return: False if this is not a known field, iTrue if it is.
                 field name.
        """
        if fields is None:
            fields = self.getFieldInfo()
        if isinstance(name, dict):
            if 'field' in name:
                name = name['field']
            elif ('func' in name or 'lfunc' in name) and allowFunc:
                return self.isFunction(name, fields) is not False
            else:
                return False
        for field in fields:
            if name == field.get('name'):
                return name
        return False

    def isFunction(self, func, fields=None):
        """
        Check if the specified object is a well-formed function reference.  If
        it is, return a canonical form.  Functions are dictionaries with at
        least a 'func' or 'lfunc' in the dictionary for left values and 'rfunc'
        for right values.  Functions optionally have a corresponding
        '(l|r|)param' key which contains either a single value or a list.  Each
        entry in the list contains a value or a dictionary, where the
        dictionary contains either another left function, a 'field' parameter,
        or a 'value' parameter.  In the canonical form, this is always a
        dictionary with 'func' and 'param', param is always a list, and the
        list always contains dictionaries.

        :param func: a dictionary containing the function specification.
        :param fields: the results from getFieldInfo.  If None, this calls
                       getFieldInfo.
        :returns: False if func is not a function specification, otherwise the
                  canonical function dictionary.
        """
        if 'func' not in func:
            return False
        result = {
            'func': func['func'],
            'param': []
        }
        param = func.get('param', [])
        if not isinstance(param, (list, tuple)):
            param = [param]
        for entry in param:
            if not isinstance(entry, dict):
                entry = {'value': entry}
            else:
                if 'value' in entry:
                    entry = {'value': entry['value']}
                elif 'field' in entry:
                    if not self.isField(entry.get('field')):
                        return False
                    entry = {'field': self.isField(entry.get('field'))}
                else:
                    entry = self.isFunction(entry)
                    if entry is False:
                        return False
            result['param'].append(entry)
        return result

    def performSelect(self, fields=[], queryProps={}, filters=[], client=None):
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

        :param fields: the results from getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           sort, fields, wait, poll, and initwait.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See above.
        """
        if queryProps.get('fields') is None:
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

    @staticmethod
    def validate(*args, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        return False

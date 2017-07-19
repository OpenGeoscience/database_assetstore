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

from girder.models.model_base import GirderException


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
    'regex': 'regex',  # case sensitive regex
    'regexp': 'regex',
    '~': 'regex',
    'not_regex': 'not_regex',
    'notregex': 'not_regex',
    'not_regexp': 'not_regex',
    'notregexp': 'not_regex',
    '!~': 'not_regex',
    'search': 'search',  # case insensitive regex
    '~*': 'search',
    'not_search': 'not_search',
    'notsearch': 'not_search',
    '!~*': 'not_search',
    'is': 'is',
    'not_is': 'not_is',
    'notis': 'not_is',
    'isnot': 'not_is',
    'is_not': 'not_is',
}
DatatypeOperators = {
    'array': {'in', 'not_in', 'is', 'not_is'},
    'boolean': {'eq', 'ne', 'in', 'not_in', 'is', 'not_is'},
    'date': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'is',
             'not_is'},
    'duration': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'is',
                 'not_is'},
    'enum': {'eq', 'ne', 'in', 'not_in', 'is', 'not_is'},
    'number': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'is',
               'not_is'},
    'string': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'regex',
               'not_regex', 'search', 'not_search', 'is', 'not_is'},
}

_connectorClasses = {}
_connectorCache = {}
_connectorCacheMaxSize = 10  # Probably should make this configurable


def getDBConnectorClass(name):
    """
    Get a DB connector class.  This checks if such a class exists and either
    returns a reference to the class or None.

    :param name: name of the connector class, as registered by __init__.
    :return: the connector class or None
    """
    return _connectorClasses.get(name, {}).get('class')


def getDBConnectorClassFromDialect(dialect, name=None):
    """
    Get a DB connector class and preferred dialect.  This checks if such a
    class exists and either returns a class and dialect name or None.

    :param dialect: name of a dialect.
    :param name: name of the DB connector.  If None, all DB connectors are
                 checked.
    :return: the preferred dialect name or None.
    :return: the connector class name or None.
    """
    # Sort our classes by priority (lower is higher priority) and find the
    # first class that has the specified dialect.
    classes = [record[-1] for record in sorted([
        (_connectorClasses[cname]['dialect'].get('priority', 0), cname)
        for cname in _connectorClasses if cname == name or not name])]
    for name in classes:
        dialects = _connectorClasses[name]['dialect'].get('dialects', {})
        if dialect in dialects:
            return dialects[dialect], name
    if dialect in classes:
        return _connectorClasses[dialect]['dialect'].get(
            'default_dialect', dialect), dialect
    return None, None


def registerConnectorClass(name, cls, dialects):
    """
    Register a connector class with a specific name.

    :param name: the name to register.  This is what an item has to specify in
                 the 'type' field.
    :param cls: a reference to the connector class.
    :param dialects: a dictionary of dialect names that this class handles.
                     The keys are allowed dialect names, and the values are
                     the dialect names that should be used.
    """
    _connectorClasses[name] = {'class': cls, 'dialect': dialects}


def clearDBConnectorCache(id):
    """
    If the id has been used in the connector cache, discard the entry.

    :param id: key for the connector cache.
    """
    id = str(id)
    if id in _connectorCache:
        _connectorCache.pop(id, None)
        return True
    return False


def getDBConnector(id, dbinfo):
    """
    Get a specific DB connector, caching it if possible.

    :param id: key for the connector cache.  None to never use the cache.
    :param dbinfo: a dictionary of information to pass to the connector
    :return: the connector instance, or None if none available.
    """
    if id is not None:
        id = str(id)
    conn = _connectorCache.get(id, None)
    if conn is None:
        connClass = getDBConnectorClass(dbinfo.get('type'))
        if connClass is None:
            return None
        conn = connClass(**dbinfo)
        if not getattr(conn, 'initialized', None):
            return None
        if id is not None:
            if len(_connectorCache) > _connectorCacheMaxSize:
                _connectorCache.clear()
            _connectorCache[id] = conn
    return conn


class DatabaseConnectorException(GirderException):
    pass


class DatabaseConnector(object):
    # If a database connector can query available databases, set this to false
    databaseNameRequired = True

    def __init__(self, *args, **kwargs):
        if not self.validate(**kwargs):
            raise DatabaseConnectorException(
                'Failed to validate database connector.')
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

    @staticmethod
    def getTableList(url, internalTables=False, **kwargs):
        """
        Get a list of known databases, each of which has a list of known tables
        from the database.  This is of the form [{'database': (database 1),
        'tables': [...]}, {'database': (database 2), 'tables': [...]}, ...].
        Each table entry is of the form {'table': (table 1), 'name': (name 1)}
        and may contain additonal connection information, such as schema.

        :param url: url to connect to the database.
        :param internaltables: True to return tables about the database itself.
        :returns: A list of known tables.
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
            elif 'func' in name and allowFunc:
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
        param = func.get('param', func.get('params', []))
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
          limit: the limit used in the query.  Negative or None for all.
          offset: the offset used in the query
          sort: the list of sort parameters used in the query.
          fields: a list of the fields that are being returned in the order
        that they are returned.
          data: a list with one entry per row of results.  Each entry is a list
        with one entry per column.

        :param fields: the results from getFieldInfo.
        :param queryProps: general query properties, including limit, offset,
                           sort, fields, group, wait, poll, and initwait.
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
                           sort, fields, group, wait, poll, and initwait.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See performSelect.
        """
        wait = queryProps.get('wait')
        if not wait:
            return self.performSelect(fields, queryProps, *args, **kwargs)
        if queryProps.get('initwait'):
            time.sleep(queryProps['initwait'])
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

    # Enable and override to customize how data gets dumped to json
    # @staticmethod
    # def jsonDumps(*args, **kwargs):
    #     return json.dumps(*args, **kwargs)


def databaseFromUri(uri):
    """
    Extract the name of the database from the database connection uri.  If
    there is no database, return None.  The uri is of the form
    (dialect)://[(user name)[:(password)]@](server)[:(port)]
    [/[(database)[/]]][?(options)]

    :param uri: the database connection uri.
    :returns: the name of the database or None.
    """
    parts = uri.split('://', 1)[-1].split('/')
    if len(parts) < 2 or not parts[1]:
        return None
    return parts[1]

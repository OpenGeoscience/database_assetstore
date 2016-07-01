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

import csv
import json
import time

from girder.models.model_base import GirderException


dbFormatList = {
    'list': 'application/json',
    'dict': 'application/json',
    'csv': 'text/csv',
}

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
    'array': {'in', 'not_in'},
    'boolean': {'eq', 'ne', 'in', 'not_in'},
    'date': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'duration': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'enum': {'eq', 'ne', 'in', 'not_in'},
    'number': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in'},
    'string': {'eq', 'ne', 'gte', 'gt', 'lt', 'lte', 'in', 'not_in', 'regex',
               'not_regex', 'search', 'not_search'},
}

_connectorClasses = {}
_connectorCache = {}


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
    :return: the connector class name or None..
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


class DatabaseConnectorException(GirderException):
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

    @staticmethod
    def getTableList(url, **kwargs):
        """
        Get a list of known table from the database.

        :param url: url to connect to the database.
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


class DatabaseQueryException(GirderException):
    pass


# Functions related to querying databases

def convertSelectDataToCSVGenerator(result):
    """
    Return a function that produces a generator for outputting a CSV file.

    :param result: the initial select results.
    :returns: a function that outputs a generator.
    """
    class Echo(object):
        def write(self, value):
            return value

    writer = csv.writer(Echo())

    def resultFunc():
        yield writer.writerow(result['fields'])
        for row in result['data']:
            yield writer.writerow(row)

    return resultFunc


def convertSelectDataToDict(result):
    """
    Convert data in list format to dictionary format.  The column names are
    used as the keys for each row.

    :param result: the initial select results.
    :returns: the results with data converted from a list of lists to a list of
              dictionaries.
    """
    columns = {result['columns'][col]: col for col in result['columns']}
    result['data'] = [{columns[i]: row[i] for i in range(len(row))}
                      for row in result['data']]
    result['format'] = 'dict'
    return result


def getFilters(conn, fields, filtersValue=None, queryParams={},
               reservedParameters=[]):
    """
    Get a set of filters from a JSON list and/or from a set of query
    parameters.  Only query parameters of the form (field)[_(operator)] where
    the entire name is not in the reserver parameter list are processed.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  This is conn.getFieldInfo().
    :filtersValue: a JSON object with the desired filters or None or empty
                   string.
    :queryParameters: a dictionary of query parameters that can add additional
                      filters.
    :reservedParameters: a list or set of reserver parameter names.
    """
    filters = []
    if filtersValue not in (None, ''):
        try:
            filtersList = json.loads(filtersValue)
        except ValueError:
            filtersList = None
        if not isinstance(filtersList, list):
            raise DatabaseQueryException(
                'The filters parameter must be a JSON list.')
        for filter in filtersList:
            filters.append(validateFilter(conn, fields, filter))
    if queryParams:
        for fieldEntry in fields:
            field = fieldEntry['name']
            for operator in FilterOperators:
                param = field + ('' if operator is None else '_' + operator)
                if param in queryParams and param not in reservedParameters:
                    filters.append(validateFilter(conn, fields, {
                        'field': field,
                        'operator': operator,
                        'value': queryParams[param]
                    }))
    return filters


def getFieldsList(conn, fields=None, fieldsValue=None):
    """
    Get a list of fields from the query parameters.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  None to let the connector fetch
                   them.
    :param fieldsValue: either a comma-separated list, a JSON list, or None.
    :returns: a list of fields or None.
    """
    if fieldsValue is None or fieldsValue == '':
        return None
    if '[' not in fieldsValue:
        fieldsList = [field.strip() for field in fieldsValue.split(',')
                      if len(field.strip())]
    else:
        try:
            fieldsList = json.loads(fieldsValue)
        except ValueError:
            fieldsList = None
        if not isinstance(fieldsList, list):
            raise DatabaseQueryException(
                'The fields parameter must be a JSON list or a '
                'comma-separated list of known field names.')
    for field in fieldsList:
        if not conn.isField(
                field, fields,
                allowFunc=getattr(conn, 'allowFieldFunctions', False)):
            raise DatabaseQueryException('Fields must use known fields %r.')
    return fieldsList


def getSortList(conn, fields=None, sortValue=None, sortDir=None):
    """
    Get a list of sort fields and directions from the query parameters.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  None to let the connector fetch
                   them.
    :param sortValue: either a sort field, a JSON list, or None.
    :param sortDir: if sortValue is a sort field, the sort direction.
    :returns: a list of sort parameters or None.
    """
    if sortValue is None or sortValue == '':
        return None
    sort = None
    if '[' not in sortValue:
        if conn.isField(sortValue, fields) is not False:
            sort = [(
                sortValue,
                -1 if sortDir in (-1, '-1', 'desc', 'DESC') else 1
            )]
    else:
        try:
            sortList = json.loads(sortValue)
        except ValueError:
            sortList = None
        if not isinstance(sortList, list):
            raise DatabaseQueryException(
                'The sort parameter must be a JSON list or a known field '
                'name.')
        sort = []
        for entry in sortList:
            if (isinstance(entry, list) and 1 <= len(entry) <= 2 and
                    conn.isField(
                        entry[0], fields,
                        allowFunc=getattr(conn, 'allowSortFunctions', False))
                    is not False):
                sort.append((
                    entry[0],
                    -1 if len(entry) > 1 and entry[1] in
                    (-1, '-1', 'desc', 'DESC') else 1
                ))
            elif (conn.isField(
                    entry, fields,
                    allowFunc=getattr(conn, 'allowSortFunctions', False))
                    is not False):
                sort.append((entry, 1))
            else:
                sort = None
                break
    if sort is None:
        raise DatabaseQueryException('Sort must use known fields.')
    return sort


def queryDatabase(id, dbinfo, params):
    """
    Query a database.

    :param id: an id used to cache the DB connector.
    :param dbinfo: a dictionary of connection information for the db.  Needs
        type, url, and either table or connection.
    :param params: query parameters.  See the select endpoint for
        documentation.
    :returns: a result function that returns a generator that yields the
        results, or None for failed.
    :returns: the mime type of the results, or None for failed.
    """
    conn = getDBConnector(id, dbinfo)
    if not conn:
        raise DatabaseConnectorException('Failed to connect to database.')
    fields = conn.getFieldInfo()
    queryProps = {
        'limit': int(50 if params.get('limit') is None
                     else params.get('limit')),
        'offset': int(params.get('offset', 0) or 0),
        'sort': getSortList(conn, fields, params.get('sort'),
                            params.get('sortdir')),
        'fields': getFieldsList(conn, fields, params.get('fields')),
        'wait': float(params.get('wait', 0)),
        'poll': float(params.get('poll', 10)),
        'initwait': float(params.get('initwait', 0)),
    }
    client = params.get('clientid')
    format = params.get('format')
    filters = getFilters(conn, fields, params.get('filters'), params, {
        'limit', 'offset', 'sort', 'sortdir', 'fields', 'wait', 'poll',
        'initwait', 'clientid', 'filters', 'format'})
    result = conn.performSelectWithPolling(fields, queryProps, filters,
                                           client)
    if result is None:
        return None, None
    if 'fields' in result:
        result['columns'] = {
            result['fields'][col] if not isinstance(
                result['fields'][col], dict) else
            result['fields'][col].get('reference', 'column_' + str(col)):
            col for col in range(len(result['fields']))}
    result['datacount'] = len(result.get('data', []))
    result['format'] = 'list'
    mimeType = 'application/json'
    if format == 'dict':
        result = convertSelectDataToDict(result)

    if format == 'csv':
        resultFunc = convertSelectDataToCSVGenerator(result)
    else:
        # We could let Girder convert the results into JSON, but it is
        # marginally faster to dump the JSON ourselves, since we can exclude
        # sorting and reduce whitespace.
        def resultFunc():
            yield json.dumps(result, check_circular=False,
                             separators=(',', ':'), sort_keys=False,
                             default=str)

    return resultFunc, mimeType


def validateFilter(conn, fields, filter):
    """
    Validate a filter by ensuring that the field exists, the operator is valid
    for that field's data type, and that any additional properties are allowed.
    Convert the filter into a fully populated dictionary style (one that has at
    least field, operator, and value).

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.
    :param filter: either a dictionary or a list or tuple with two to three
                   components representing (field), [(operator),] (value).
    :returns filter: the filter in dictionary-style.
    """
    if isinstance(filter, (list, tuple)):
        if len(filter) < 2 or len(filter) > 3:
            raise DatabaseQueryException(
                'Filters in list format must have two or three components.')
        if len(filter) == 2:
            filter = {'field': filter[0], 'value': filter[1]}
        else:
            filter = {
                'field': filter[0], 'operator': filter[1], 'value': filter[2]
            }
    if filter.get('operator') not in FilterOperators:
        raise DatabaseQueryException('Unknown filter operator %r' % filter.get(
            'operator'))
    filter['operator'] = FilterOperators[filter.get('operator')]
    if 'field' not in filter and 'lvalue' in filter:
        filter['field'] = {'value': filter['lvalue']}
    if 'field' not in filter and ('func' in filter or 'lfunc' in filter):
        filter['field'] = {
            'func': filter.get('func', filter.get('lfunc')),
            'param': filter.get('param', filter.get('params', filter.get(
                'lparam', filter.get('lparams'))))
        }
    if 'value' not in filter and 'rfunc' in filter:
        filter['value'] = {
            'func': filter['rfunc'],
            'param': filter.get('rparam', filter.get('rparams'))
        }
    if 'field' not in filter:
        raise DatabaseQueryException('Filter must specify a field or func.')
    if (not conn.isField(
            filter['field'], fields,
            allowFunc=getattr(conn, 'allowFilterFunctions', False)) and
            not isinstance(filter['field'], dict) and
            'value' not in filter['field'] and 'func' not in filter['field']):
        raise DatabaseQueryException('Filters must be on known fields.')
    if not filter.get('value'):
        raise DatabaseQueryException('Filters must have a value or rfunc.')
    if not conn.checkOperatorDatatype(filter['field'], filter['operator'],
                                      fields):
        raise DatabaseQueryException('Cannot use %s operator on field %s' % (
            filter['operator'], filter['field']))
    return filter

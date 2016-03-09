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

import cherrypy
import datetime
import json
import six

from six.moves import range

from girder.api import access
from girder.api.v1.item import Item
from girder.api.describe import describeRoute, Description
from girder.api.rest import filtermodel, loadmodel, RestException
from girder.models.model_base import AccessType

from . import dbs


dbInfoKey = 'databaseMetadata'


def getFilters(conn, fields=None, filtersValue=None, queryParams={},
               reservedParameters=[]):
    """
    Get a set of filters from a JSON list and/or from a set of query
    parameters.  Only query parameters of the form (field)[_(operator)] where
    the entire name is not in the reserver parameter list are processed.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  None to let the connector fetch
                   them.
    :filtersValue: a JSON object with the desired filters or None or empty
                   string.
    :queryParameters: a dictionary of query parameters that can add additional
                      filters.
    :reservedParameters: a list or set of reserver parameter names.
    """
    if not fields:
        fields = conn.getFieldInfo()
    filters = []
    if filtersValue not in (None, ''):
        try:
            filtersList = json.loads(filtersValue)
        except ValueError:
            filtersList = None
        if not isinstance(filtersList, list):
            raise RestException('The filters parameter must be a JSON list.')
        for filter in filtersList:
            filters.append(validateFilter(conn, fields, filter))
    if queryParams:
        for fieldEntry in fields:
            field = fieldEntry['name']
            for operator in dbs.FilterOperators:
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
            raise RestException('The fields parameter must be a JSON list or '
                                'a comma-separated list of known field names.')
    for field in fieldsList:
        if not conn.isField(
                field, fields,
                allowFunc=getattr(conn, 'allowFieldFunctions', False)):
            raise RestException('Fields must use known fields %r.')
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
            raise RestException('The sort parameter must be a JSON list or a '
                                'known field name.')
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
        raise RestException('Sort must use known fields.')
    return sort


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
            raise RestException('Filters in list-format must have two or '
                                'three components.')
        if len(filter) == 2:
            filter = {'field': filter[0], 'value': filter[1]}
        else:
            filter = {
                'field': filter[0], 'operator': filter[1], 'value': filter[2]
            }
    if filter.get('operator') not in dbs.FilterOperators:
        raise RestException('Unknown filter operator %r' % filter.get(
            'operator'))
    filter['operator'] = dbs.FilterOperators[filter.get('operator')]
    if 'field' not in filter and 'func' in filter:
        filter['field'] = {
            'func': filter['func'],
            'param': filter.get('param')
        }
    if 'value' not in filter and 'rfunc' in filter:
        filter['value'] = {
            'func': filter['rfunc'],
            'param': filter.get('rparam')
        }
    if not conn.isField(
            filter['field'], fields,
            allowFunc=getattr(conn, 'allowFilterFunctions', False)):
        raise RestException('Filters must be on known fields.')
    if not filter.get('value'):
        filter['value'] = None
    if not conn.checkOperatorDatatype(filter['field'], filter['operator'],
                                      fields):
        raise RestException('Cannot use %s operator on field %s' % (
            filter['operator'], filter['field']))
    return filter


class DatabaseItemResource(Item):

    def __init__(self, apiRoot):
        # Don't call the parent (Item) constructor, to avoid redefining routes,
        # but do call the grandparent (Resource) constructor
        super(Item, self).__init__()

        self.resourceName = 'item'
        apiRoot.item.route('GET', (':id', 'database'), self.getDatabaseLink)
        apiRoot.item.route('POST', (':id', 'database'),
                           self.createDatabaseLink)
        apiRoot.item.route('DELETE', (':id', 'database'),
                           self.deleteDatabaseLink)
        apiRoot.item.route('GET', (':id', 'database', 'fields'),
                           self.getDatabaseFields)
        apiRoot.item.route('PUT', (':id', 'database', 'refresh'),
                           self.databaseRefresh)
        apiRoot.item.route('GET', (':id', 'database', 'select'),
                           self.databaseSelect)

    @describeRoute(
        Description('Get item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.READ)
    def getDatabaseLink(self, item, params):
        if self.model('item').hasAccess(item, self.getCurrentUser(),
                                        AccessType.WRITE):
            return item.get(dbInfoKey)
        else:
            return item.get(dbInfoKey) is not None

    @describeRoute(
        Description('Set or modify item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
        .param('body', 'A JSON object containing the database information to '
               'update. At a minimum this must include "type", "uri", and '
               '"table".', paramType='body')
        .notes('Set database information fields to null to delete them.')
        .errorResponse('ID was invalid.')
        .errorResponse('Invalid JSON passed in request body.')
        .errorResponse('Unknown database type.')
        .errorResponse('Database information is invalid.')
        .errorResponse('Write access was denied for the item.', 403)
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.ADMIN)
    @filtermodel(model='item')
    def createDatabaseLink(self, item, params):
        dbs.clearDBConnectorCache(item['_id'])
        dbinfo = self.getBodyJson()
        if dbInfoKey not in item:
            item[dbInfoKey] = {}
        item[dbInfoKey].update(six.viewitems(dbinfo))
        toDelete = [k for k, v in six.viewitems(item[dbInfoKey]) if v is None]
        for key in toDelete:
            del item[dbInfoKey][key]
        item['updated'] = datetime.datetime.utcnow()
        dbinfo = item[dbInfoKey]
        # Generate type set from connector classes
        connClass = dbs.getDBConnectorClass(dbinfo.get('type'))
        if not connClass:
            raise RestException('Unknown database type.')
        if not connClass.validate(**dbinfo):
            raise RestException('Database information is invalid.')
        # ##DWM:: check if the database is reachable, and warn if it isn't?
        return self.model('item').save(item)

    @describeRoute(
        Description('Delete item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.WRITE)
    def deleteDatabaseLink(self, item, params):
        dbs.clearDBConnectorCache(item['_id'])
        deleted = False
        if dbInfoKey in item:
            del item[dbInfoKey]
            self.model('item').save(item)
            deleted = True
        return {
            'deleted': deleted
        }

    @describeRoute(
        Description('Get information on the fields available for an item '
                    'database link.')
        .param('id', 'The ID of the item.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
        .errorResponse('Item is not a database link.')
        .errorResponse('Failed to connect to database.')
    )
    @access.cookie
    @access.public
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.READ)
    def getDatabaseFields(self, item, params):
        dbinfo = item.get(dbInfoKey)
        if not dbinfo:
            raise RestException('Item is not a database link.')
        conn = dbs.getDBConnector(item['_id'], dbinfo)
        if not conn:
            raise RestException('Failed to connect to database.')
        fields = conn.getFieldInfo()
        # ##DWM:: filter based on user?
        return fields

    @describeRoute(
        Description('Refresh data associated with an item database link.')
        .param('id', 'The ID of the item.', paramType='path')
        .notes('This may be necessary if fields (columns) within the linked '
               'table are added, dropped, or changed, or if the available '
               'database functions are altered.')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
        .errorResponse('Item is not a database link.')
    )
    @access.public
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.READ)
    def databaseRefresh(self, item, params):
        dbinfo = item.get(dbInfoKey)
        if not dbinfo:
            raise RestException('Item is not a database link.')
        result = dbs.clearDBConnectorCache(item['_id'])
        return result

    @describeRoute(
        Description('Get data from a database link.')
        .param('id', 'The ID of the item.', paramType='path')
        .param('limit', 'Result set size limit (default=50).',
               required=False, dataType='int')
        .param('offset', 'Offset into result set (default=0).',
               required=False, dataType='int')
        .param('sort', 'Either a field to sort the results by or a JSON list '
               'of multiple fields and directions for sorting the results '
               '(e.g., [["field1", 1], ["field2", -1]])', required=False)
        .param('sortdir', '1 for ascending, -1 for descending (default=1).  '
               'Ignored if sort is unspecified or is a JSON list.',
               required=False, dataType='int')
        .param('fields', 'A comma-separated or JSON list of fields (column '
               'names) to return (default is all fields).  If a JSON list is '
               'used, instead of a plain string, a field may be a dictionary '
               'with a function definition and an optional "reference" entry '
               'which is used to identify the resultant column.',
               required=False)
        .param('format', 'The format to return the data (default is '
               'list).', required=False, enum=['list', 'dict'])
        .param('clientid', 'A string to use for a client id.  If specified '
               'and there is an extant query to this end point from the same '
               'clientid, the extant query will be cancelled.', required=False)
        .param('wait', 'Maximum duration in seconds to wait for data '
               '(default=0).  If a positive value is specified and the '
               'initial query returns no results, the query will be repeated '
               'every (poll) seconds until this time elapses or there are '
               'some results.', required=False, dataType='float', default=0)
        .param('poll', 'Minimum interval in seconds between checking for data '
               'when waiting (default=10).', required=False, dataType='float',
               default=10)
        .param('initwait', 'When waiting, initial delay in seconds before '
               'starting to poll for more data.  This is not counted as part '
               'of the wait duration (default=0).', required=False,
               dataType='float', default=0)
        .param('filters', 'A JSON list of filters to apply to the data.  Each '
               'entry in the list can be either a list or a dictionary.  If a '
               'list, it contains [(field), (operator), (value)], where '
               '(operator) is optional.  If a dictionary, at least the '
               '"field" and "value" keys must contain values, and "operator" '
               'and "function" keys can also be added.', required=False)
        .notes('Instead of or in addition to specifying a filters parameter, '
               'additional query parameters of the form (field)[_(operator)]='
               '(value) can be used.  '
               'Operators depend on the data type of the field, and include = '
               '(no operator or eq), != (<>, ne), >= (min, gte), <= (lte), > '
               '(gt), < (max, lt), in, notin, ~ (regex), ~* (search -- '
               'typically a case insensitive regex or word-stem search), !~ '
               '(notregex), !~* (notsearch).  '
               'If the backing database connector supports it, any place a '
               'field can be used can be replaced with a function reference.  '
               'This is a dictionary with "func" or with the name of the '
               'database function and "params" which is a list of values, '
               'fields, or functions to pass to the function.  If the param '
               'entry is not a dictionary, it is treated as a value.  If a '
               'dictionary, it can contain "value", "field", or "func" and '
               '"param".')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
        .errorResponse('Item is not a database link.')
        .errorResponse('Failed to connect to database.')
        .errorResponse('The sort parameter must be a JSON list or a known '
                       'field name.')
        .errorResponse('Sort must use known fields.')
        .errorResponse('The fields parameter must be a JSON list or a '
                       'comma-separated list of known field names.')
        .errorResponse('Fields must use known fields.')
        .errorResponse('The filters parameter must be a JSON list.')
        .errorResponse('Filters in list-format must have two or three '
                       'components.')
        .errorResponse('Unknown filter operator')
        .errorResponse('Filters must be on known fields.')
        .errorResponse('Cannot use operator on field')
    )
    @access.cookie
    @access.public
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.READ)
    def databaseSelect(self, item, params):
        dbinfo = item.get(dbInfoKey)
        if not dbinfo:
            raise RestException('Item is not a database link.')
        conn = dbs.getDBConnector(item['_id'], dbinfo)
        if not conn:
            raise RestException('Failed to connect to database.')
        fields = conn.getFieldInfo()
        queryProps = {
            'limit': int(params.get('limit', 50)),
            'offset': int(params.get('offset', 0)),
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
        queryInfo = conn.prepareSelect(fields, queryProps, filters, client)
        result = conn.performSelectWithPolling(fields, queryProps, filters,
                                               client, queryInfo)
        if result is None:
            cherrypy.response.status = 500
            return
        if 'fields' in result:
            result['columns'] = {
                result['fields'][col] if not isinstance(
                    result['fields'][col], dict) else
                result['fields'][col].get('reference', 'column_' + str(col)):
                col for col in range(len(result['fields']))}
        result['datacount'] = len(result.get('data', []))
        result['format'] = 'list'
        if format == 'dict':
            pass  # ##DWM::
            # result = convertSelectDataToDict(result)
            # ##DWM::

        # We could let Girder convert the results into JSON, but it is
        # marginally faster to dump the JSON ourselves, since we can
        # exclude sorting and reduce whitespace.
        def resultFunc():
            yield json.dumps(result, check_circular=False,
                             separators=(',', ':'), sort_keys=False,
                             default=str)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return resultFunc

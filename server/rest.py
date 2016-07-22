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

from girder.api import access
from girder.api.v1.file import File
from girder.api.describe import describeRoute, Description
from girder.api.rest import filtermodel, loadmodel, Resource, RestException
from girder.models.model_base import AccessType
from girder.utility import assetstore_utilities
from girder.utility.progress import ProgressContext

from . import assetstore, dbs
from .assetstore import dbInfoKey
from .query import DatabaseQueryException, dbFormatList, queryDatabase


class DatabaseFileResource(File):

    def __init__(self, apiRoot):
        # Don't call the parent (File) constructor, to avoid redefining routes,
        # but do call the grandparent (Resource) constructor
        super(File, self).__init__()

        self.resourceName = 'file'
        apiRoot.file.route('GET', (':id', 'database'), self.getDatabaseLink)
        apiRoot.file.route('POST', (':id', 'database'),
                           self.createDatabaseLink)
        apiRoot.file.route('GET', (':id', 'database', 'fields'),
                           self.getDatabaseFields)
        apiRoot.file.route('PUT', (':id', 'database', 'refresh'),
                           self.databaseRefresh)
        apiRoot.file.route('GET', (':id', 'database', 'select'),
                           self.databaseSelect)

    @describeRoute(
        Description('Get file database link information.')
        .param('id', 'The ID of the file.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the file.', 403)
    )
    @access.user
    @loadmodel(model='file', map={'id': 'file'}, level=AccessType.READ)
    def getDatabaseLink(self, file, params):
        if self.model('file').hasAccess(file, self.getCurrentUser(),
                                        AccessType.WRITE):
            return file.get(dbInfoKey)
        else:
            return file.get(dbInfoKey) is not None

    @describeRoute(
        Description('Set or modify file database link information.')
        .param('id', 'The ID of the file.', paramType='path')
        .param('body', 'A JSON object containing the database information to '
               'update. At a minimum this must include "table" or '
               '"collection".', paramType='body')
        .notes('Set database information fields to null to delete them.')
        .errorResponse('ID was invalid.')
        .errorResponse('Invalid JSON passed in request body.')
        .errorResponse('Write access was denied for the file.', 403)
    )
    @access.user
    @loadmodel(model='file', map={'id': 'file'}, level=AccessType.ADMIN)
    @filtermodel(model='file')
    def createDatabaseLink(self, file, params):
        dbs.clearDBConnectorCache(file['_id'])
        dbinfo = self.getBodyJson()
        if dbInfoKey not in file:
            file[dbInfoKey] = {}
        file[dbInfoKey].update(six.viewitems(dbinfo))
        toDelete = [k for k, v in six.viewitems(file[dbInfoKey]) if v is None]
        for key in toDelete:
            del file[dbInfoKey][key]
        file['updated'] = datetime.datetime.utcnow()
        dbinfo = file[dbInfoKey]
        return self.model('file').save(file)

    @describeRoute(
        Description('Get information on the fields available for an file '
                    'database link.')
        .param('id', 'The ID of the file.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the file.', 403)
        .errorResponse('File is not a database link.')
        .errorResponse('Failed to connect to database.')
    )
    @access.cookie
    @access.public
    @loadmodel(model='file', map={'id': 'file'}, level=AccessType.READ)
    def getDatabaseFields(self, file, params):
        dbinfo = assetstore.getDbInfoForFile(file)
        if not dbinfo:
            raise RestException('File is not a database link.')
        conn = dbs.getDBConnector(file['_id'], dbinfo)
        fields = conn.getFieldInfo()
        return fields

    @describeRoute(
        Description('Refresh data associated with an file database link.')
        .param('id', 'The ID of the file.', paramType='path')
        .notes('This may be necessary if fields (columns) within the linked '
               'table are added, dropped, or changed, or if the available '
               'database functions are altered.')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the file.', 403)
        .errorResponse('File is not a database link.')
    )
    @access.public
    @loadmodel(model='file', map={'id': 'file'}, level=AccessType.READ)
    def databaseRefresh(self, file, params):
        dbinfo = assetstore.getDbInfoForFile(file)
        if not dbinfo:
            raise RestException('File is not a database link.')
        result = dbs.clearDBConnectorCache(file['_id'])
        return {
            'refreshed': result
        }

    @describeRoute(
        Description('Get data from a database link.')
        .param('id', 'The ID of the file.', paramType='path')
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
        .param('filters', 'A JSON list of filters to apply to the data.  Each '
               'entry in the list can be either a list or a dictionary.  If a '
               'list, it contains [(field), (operator), (value)], where '
               '(operator) is optional.  If a dictionary, at least the '
               '"field" and "value" keys must contain values, and "operator" '
               'and "function" keys can also be added.', required=False)
        .param('format', 'The format to return the data (default=list).',
               required=False, enum=list(dbFormatList))
        .param('pretty', 'If true, add whitespace to JSON outputs '
               '(default=false).', required=False, dataType='boolean')
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
        .errorResponse('Read access was denied for the file.', 403)
        .errorResponse('File is not a database link.')
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
    @loadmodel(model='file', map={'id': 'file'}, level=AccessType.READ)
    def databaseSelect(self, file, params):
        dbinfo = assetstore.getDbInfoForFile(file)
        if not dbinfo:
            raise RestException('File is not a database link.')
        queryparams = assetstore.getQueryParamsForFile(file)
        queryparams.update(params)
        try:
            resultFunc, mimeType = queryDatabase(
                file['_id'], dbinfo, queryparams)
        except DatabaseQueryException as exc:
            raise RestException(exc.message)
        if resultFunc is None:
            cherrypy.response.status = 500
            return
        cherrypy.response.headers['Content-Type'] = mimeType
        return resultFunc


class DatabaseAssetstoreResource(Resource):
    def __init__(self):
        super(DatabaseAssetstoreResource, self).__init__()
        self.resourceName = 'database_assetstore'
        self.route('GET', (':id', 'tables'), self.getTables)
        self.route('PUT', (':id', 'import'), self.importData)

    def _getTableList(self, assetstore):
        """
        Given an assetstore, return the list of known tables or collections.

        :param assetstore: the assetstore document.
        :returns: a list of known tables.
        """
        cls = dbs.getDBConnectorClass(assetstore['database']['dbtype'])
        return cls.getTableList(
            assetstore['database']['uri'],
            dbparams=assetstore['database'].get('dbparams', {}))

    def _parseTableList(self, tables, assetstore):
        """
        Given a list which can include plain strings and objects with
        optionally database, name, and table, find the set of matching
        databases and tables from the assetstore.

        :param tables: the input list of table names, '', objects with
            database and possibly name keys, and objects with a table key.
        :param assetstore: the assetstore document.
        :returns: the list of table references with database and other
            parameters as needed.
        """
        all = '' in tables or None in tables
        tables = [{'name': table} if isinstance(table, six.string_types)
                  else table for table in tables if table != '']
        results = [table for table in tables if table.get('table') and not all]
        tables = [table for table in tables if not table.get('table')]
        if not len(tables) and not all:
            return results
        tableList = self._getTableList(assetstore)
        defaultDatabase = dbs.databaseFromUri(assetstore['database']['uri'])
        for database in tableList:
            for tableEntry in database['tables']:
                use = all
                for table in tables:
                    if (database['database'] ==
                            table.get('database', defaultDatabase) and
                            (not table.get('name') or table.get('name') ==
                             tableEntry.get('name', tableEntry['table']))):
                        use = True
                if use:
                    entry = tableEntry.copy()
                    if not defaultDatabase:
                        entry['database'] = database['database']
                    results.append(entry)
        return results

    @access.admin
    @loadmodel(model='assetstore')
    @describeRoute(
        Description('Get a list of tables or collections from a database.')
        .notes('Only site administrators may use this endpoint.')
        .param('id', 'The ID of the assetstore representing the Database.',
               paramType='path')
        .errorResponse()
        .errorResponse('You are not an administrator.', 403)
    )
    def getTables(self, assetstore, params):
        return self._getTableList(assetstore)

    @access.admin
    @loadmodel(model='assetstore')
    @describeRoute(
        Description('Import tables (also called collections) from a database '
                    'assetstore to files.')
        .notes('Only site administrators may use this endpoint.')
        .param('id', 'The ID of the assetstore representing the database(s).',
               paramType='path')
        .param('parentId', 'The ID of the parent folder, collection, or user '
               'in the Girder data hierarchy under which to import the files.')
        .param('parentType', 'The type of the parent object to import into.',
               enum=('folder', 'user', 'collection'), required=False)
        .param('table', 'The name of a single table, or a JSON list.  Each '
               'entry of the list is either a table name, an object with '
               '\'database\' and \'name\' keys, or an object with at least a '
               '\'table\' key.  If a table key is specified, the entire '
               'object is used as the specification for table routing.  '
               'Otherwise, if a database is specified without a name, all '
               'tables from the database are imported.  If not specified or '
               'an empty string is in the list, the assetstore will be '
               'inspected and all tables will be imported.', required=False)
        .param('sort', 'The default sort to use.  Either a field name or a '
               'JSON list of fields and directions.', required=False)
        .param('fields', 'The default fields to return.', required=False)
        .param('filters', 'The default fields to return.', required=False)
        .param('limit', 'The default limit of rows to return.', required=False,
               dataType='int')
        .param('format', 'The default format return.', required=False,
               enum=list(dbFormatList))
        .param('progress', 'Whether to record progress on this operation ('
               'default=False)', required=False, dataType='boolean')
        .errorResponse()
        .errorResponse('You are not an administrator.', 403)
    )
    def importData(self, assetstore, params):
        self.requireParams(('parentId'), params)

        user = self.getCurrentUser()

        parentType = params.get('parentType', 'folder')
        if parentType not in ('user', 'collection', 'folder'):
            raise RestException('Invalid parentType.')
        parent = self.model(parentType).load(params['parentId'], force=True,
                                             exc=True)
        tables = params.get('table', '')
        if '[' in tables:
            try:
                tables = json.loads(tables)
                if not isinstance(tables, list):
                    raise ValueError()
            except ValueError:
                raise RestException('The table parameter must either be the '
                                    'name of a table or a JSON list.')
        else:
            tables = [tables]
        if params.get('limit'):
            try:
                params['limit'] = int(params['limit'])
                if params['limit'] <= 0:
                    raise ValueError()
            except ValueError:
                raise RestException('The limit must be a positive integer.')
        if '' in tables:
            tables = ['']
        tables = self._parseTableList(tables, assetstore)
        if not len(tables):
            raise RestException(
                'The list of tables must have at least one value.')
        format = params.get('format')
        if not format:
            format = 'list'
        if format not in dbFormatList:
            raise RestException(
                'Format must be one of %s.' % ', '.join(list(dbFormatList)))

        progress = self.boolParam('progress', params, default=False)

        adapter = assetstore_utilities.getAssetstoreAdapter(assetstore)

        with ProgressContext(
                progress, user=user,
                title='Importing data from Database assetstore') as ctx:
            adapter.importData(parent, parentType, {
                'tables': tables,
                'sort': params.get('sort'),
                'fields': params.get('fields'),
                'filters': params.get('filters'),
                'limit': params.get('limit') or None,
                'format': format
                }, ctx, user)

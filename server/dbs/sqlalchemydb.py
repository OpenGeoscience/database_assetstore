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

import six
import sqlalchemy
import sqlalchemy.engine.reflection
import sqlalchemy.orm
import time

from six.moves import range

from girder import logger as log

from . import base
from .base import DatabaseConnectorException


DatabaseOperators = {
    'eq': '=',
    'ne': '!=',
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
}


_enginePool = {}
_enginePoolMaxSize = 5


def getEngine(url, **kwargs):
    """
    Get a sqlalchem engine from a pool in case we use the same parameters for
    multiple connections.
    """
    key = (url, frozenset(six.viewitems(kwargs)))
    engine = _enginePool.get(key)
    if engine is None:
        engine = sqlalchemy.create_engine(url, **kwargs)
        if len(_enginePool) >= _enginePoolMaxSize:
            _enginePoolMaxSize.clear()
        _enginePool[key] = engine
    return engine


class SQLAlchemyConnector(base.DatabaseConnector):
    name = 'sqlalchemy'

    def __init__(self, *args, **kwargs):
        super(SQLAlchemyConnector, self).__init__(*args, **kwargs)
        if not self.validate(**kwargs):
            return
        self.table = kwargs.get('table')
        self.schema = kwargs.get('schema')
        self.dbEngine = None
        self.sessions = {}
        # dbparams can include values in http://www.postgresql.org/docs/
        #   current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        self.dbparams = kwargs.get('dbparams', {})
        self.databaseUrl = kwargs.get('url')
        # If the prefix is sql://, use the default generic-sql dialect
        if self.databaseUrl.startswith('sql://'):
            self.databaseUrl = self.databaseUrl.split('sql://', 1)[1]

        # Additional parameters:
        #  idletime: seconds after which a connection is considered idle
        #  abandontime: second after which a connection will be abandoned
        self.dbIdleTime = float(kwargs.get('idletime', 300))
        self.dbAbandonTime = float(kwargs.get('abandontime',
                                   self.dbIdleTime * 5))
        self.databaseOperators = DatabaseOperators
        self.fields = None
        self.allowFieldFunctions = True
        self.allowSortFunctions = True
        self.allowFilterFunctions = True
        self.initialized = True

        class Table(object):
            """
            This is used to handle table properties from SQLAlchemy.
            """
            pass

        self.tableClass = Table
        self._allowedFunctions = {}

    def _applyFilter(self, query, filter):
        """
        Apply a filter to a SQLAlchemy session query.

        :param query: the SQLAlchemy session query.
        :param filter: information on the filter.
        :return: a filtered session query.
        """
        operator = filter['operator']
        operator = base.FilterOperators.get(operator, operator)
        operator = self.databaseOperators.get(operator, operator)
        field = self._convertFieldOrFunction(filter['field'])
        negate = False
        if operator.startswith('not_'):
            negate = True
            operator = operator.split('not_', 1)[1]
        if operator == 'in':
            values = filter['value']
            if not isinstance(values, (list, tuple)):
                values = [values]
            values = [self._convertFieldOrFunction(value, True)
                      for value in values]
            opfunc = field.in_(values)
        else:
            value = self._convertFieldOrFunction(filter['value'], True)
            opfunc = field.op(operator)(value)
        if negate:
            opfunc = sqlalchemy.not_(opfunc)
        query = query.filter(opfunc)
        return query

    def _convertFieldOrFunction(self, fieldOrFunction, preferValue=False):
        """
        Convert a string to a column reference, or a dictionary to a column or
        function reference.  If a function is passed, this should be a
        canonical function reference ('func' and 'param' are both populated).

        :param fieldOrFunction: a string with a column name or a dictionary
                                with either a field, function, or value.
        :param preferValue: if True then if fieldOrFunction is not a
                            dictionary, return it unchanged.
        :returns: a constructed column or function object, or a bare value.
        """
        if not isinstance(fieldOrFunction, dict):
            if preferValue:
                return fieldOrFunction
            return getattr(self.tableClass, fieldOrFunction)
        if 'field' in fieldOrFunction:
            return getattr(self.tableClass, fieldOrFunction['field'])
        if 'value' in fieldOrFunction:
            if not preferValue:
                return sqlalchemy.sql.elements.literal(
                    fieldOrFunction['value'])
            return fieldOrFunction['value']
        fieldOrFunction = self.isFunction(fieldOrFunction)
        if fieldOrFunction is False:
            raise DatabaseConnectorException('Not a function')
        if not self._isFunctionAllowed(fieldOrFunction['func']):
            raise DatabaseConnectorException('Function %s is not allowed' %
                                             fieldOrFunction['func'])
        return getattr(sqlalchemy.func, fieldOrFunction['func'])(
            *[self._convertFieldOrFunction(entry, True) for entry in
              fieldOrFunction.get('param', fieldOrFunction.get('params', []))])

    def _isFunctionAllowed(self, proname):
        """
        Check if the specified function is allowed.  Currently, only
        non-volatile functions are allowed, even though there are volatile
        functions that are harmless.

        :param proname: name of the function to check.
        :returns: True is allowed, False is not.
        """
        return self._allowedFunctions.get(proname, False)

    def connect(self, client=None):
        """
        Connect to the database.

        :param client: if None, use a new session.  If specified, if this
                       client is currently marked in use, cancel the client's
                       existing query and return a connection from the pool fo
                       r the client to use.
        :return: a SQLAlchemny session object.
        """
        if not self.dbEngine:
            self.dbEngine = getEngine(self.databaseUrl, **self.dbparams)
            metadata = sqlalchemy.MetaData(self.dbEngine)
            table = sqlalchemy.Table(self.table, metadata, schema=self.schema,
                                     autoload=True)

            # The orm.mapper is used to refer to our columns.  If the table or
            # view we are connecting to does not have any primary keys, the
            # mapper will fail.  Use the first column as a fallback; this is
            # only safe because we DON'T alter data; we have no guarantee we
            # can refer to a specific row (but we don't need to).
            fallbackPrimaryCol = None
            for col in table.c:
                if col.primary_key:
                    fallbackPrimaryCol = None
                    break
                if fallbackPrimaryCol is None:
                    fallbackPrimaryCol = col

            sqlalchemy.orm.mapper(
                self.tableClass, table, primary_key=fallbackPrimaryCol)
        # If we are asking for a specific client, clean up defunct clients
        curtime = time.time()
        if client:
            for oldsess in self.sessions.keys():
                idle = curtime - self.sessions[oldsess]['last']
                if ((idle > self.dbIdleTime and
                        not self.sessions[oldsess]['used']) or
                        idle > self.dbAbandonTime):
                    del self.sessions[oldsess]
        # Cancel an existing query
        if client in self.sessions and self.sessions[client]['used']:
            self.sessions[client]['session'].connection().connection.cancel()
            self.sessions[client]['session'].rollback()
            self.sessions[client]['used'] = False
        if client in self.sessions:
            sess = self.sessions[client]['session']
            # Always ensure a fresh query
            sess.rollback()
        else:
            sess = sqlalchemy.orm.sessionmaker(bind=self.dbEngine)()
            # This is a further guard against changing the database.  It isn't
            # a real guard against change, as if we somehow allow an injection
            # attack, it could be turned off.  Also, volatile functions can
            # still have side effects (for instance, setseed() changes the
            # state for generating random numbers which could have
            # cryptographic implications).
            sess.execute('set default_transaction_read_only=on')
        if client:
            if client not in self.sessions:
                self.sessions[client] = {}
            self.sessions[client]['used'] = True
            self.sessions[client]['last'] = curtime
            self.sessions[client]['session'] = sess
        return sess

    def disconnect(self, db, client=None):
        """
        Mark that a client has finished with a database connection and it can
        be closed or reused without any issue.

        :param db: the database connection to mark as finished.
        :param client: the client that owned this connection.
        """
        if client in self.sessions:
            self.sessions[client]['used'] = False

    def getFieldInfo(self):
        """
        Return a list of fields that are known and can be queried.

        :return: a list of known fields.  Each entry is a dictionary with name,
                 datatype, and optionally a description.
        """
        if self.fields is not None:
            return self.fields
        db = self.connect()
        fields = []
        for column in sqlalchemy.orm.class_mapper(
                self.tableClass).iterate_properties:
            if (isinstance(column, sqlalchemy.orm.ColumnProperty) and
                    len(column.columns) == 1):
                try:
                    coltype = str(column.columns[0].type)
                except sqlalchemy.exc.CompileError:
                    coltype = 'unknown'
                fields.append({
                    'name': column.key,
                    'type': coltype
                })
        self.disconnect(db)
        if len(fields):
            self.fields = fields
        return fields

    @staticmethod
    def getTableList(url, dbparams={}, **kwargs):
        """
        Get a list of known databases, each of which has a list of known tables
        from the database.  This is of the form [{'database': (database),
        'tables': [{'schema': (schema), 'table': (table 1)}, ...]}]

        :param url: url to connect to the database.
        :param dbparams: optional parameters to send to the connection.
        :returns: A list of known tables.
        """
        # If the prefix is sql://, use the default generic-sql dialect
        if url.startswith('sql://'):
            url = url.split('sql://', 1)[1]
        dbEngine = sqlalchemy.create_engine(url, **dbparams)
        insp = sqlalchemy.engine.reflection.Inspector.from_engine(dbEngine)
        schemas = insp.get_schema_names()
        defaultSchema = insp.default_schema_name

        tables = [{'name': table, 'table': table}
                  for table in dbEngine.table_names()]
        tables.extend([{'name': view, 'table': view}
                       for view in insp.get_view_names()])
        databaseName = base.databaseFromUri(url)
        results = [{'database': databaseName, 'tables': tables}]
        for schema in schemas:
            if schema != defaultSchema:
                tables = [{'name': '%s.%s' % (schema, table),
                           'table': table, 'schema': schema}
                          for table in dbEngine.table_names(schema=schema)]
                tables.extend([{'name': '%s.%s' % (schema, view),
                                'table': view, 'schema': schema}
                               for view in insp.get_view_names(schema=schema)])
                results[0]['tables'].extend(tables)
        return results

    def performSelect(self, fields, queryProps={}, filters=[], client=None):
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
                           and sort.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See above.
        """
        if queryProps.get('fields') is None:
            queryProps['fields'] = [field['name'] for field in fields]
        result = {
            'limit': queryProps.get('limit'),
            'offset': queryProps.get('offset'),
            'sort': queryProps.get('sort'),
            'fields': queryProps.get('fields'),
            'data': []
        }
        sess = self.connect(client)
        query = sess.query(self.tableClass)
        for filter in filters:
            query = self._applyFilter(query, filter)
        if queryProps.get('sort'):
            sortList = []
            for pos in range(len(queryProps['sort'])):
                sort = queryProps['sort'][pos]
                sortCol = self._convertFieldOrFunction(sort[0])
                if sort[1] == -1:
                    sortCol = sortCol.desc()
                sortList.append(sortCol)
            query = query.order_by(*sortList)
        if 'limit' in queryProps:
            query = query.limit(int(queryProps['limit']))
        if 'offset' in queryProps:
            query = query.offset(int(queryProps['offset']))
        columns = [self._convertFieldOrFunction(field)
                   for field in queryProps['fields']]
        # Clone the query and set it to return the columns we are interested
        # in.  Using   result['data'] = list(query.values(*columns))   is more
        # compact and skips one internal _clone call, but doesn't allow logging
        # the actual sql used.  with_entities clears the columns we are
        # selecting (it defaults to all of the native table columns), and
        # add_columns puts back just what we want, including expressions.
        query = query.with_entities(*[])
        query = query.add_columns(*columns)
        log.info('Query: %s', ' '.join(str(query.statement.compile(
            bind=sess.get_bind(),
            compile_kwargs={'literal_binds': True})).split()))
        result['data'] = list(query)
        self.disconnect(sess, client)
        return result

    @staticmethod
    def validate(table=None, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        if not table or not kwargs.get('url'):
            return False
        # We could validate other database parameters, too
        return True


# Make a list of the dialects this module supports.  There is no default
# dialect.
_dialects = {
    'dialects': {},
    'priority': 1,
}
for dialect in getattr(sqlalchemy.dialects, '__all__', []):
    _dialects['dialects'][dialect] = dialect

base.registerConnectorClass(SQLAlchemyConnector.name, SQLAlchemyConnector,
                            _dialects)

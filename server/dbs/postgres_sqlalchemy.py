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

# To try out sqlalchemy
"""
import sqlalchemy, sqlalchemy.orm
db = sqlalchemy.create_engine('postgresql://taxi@10.0.2.2/gisdb')
m = sqlalchemy.MetaData(db)
class Table(object):
    pass

towns = sqlalchemy.Table('towns', m, autoload=True)
sqlalchemy.orm.mapper(Table, towns)
sess = sqlalchemy.orm.sessionmaker(bind=db)()
sess.query(Table).order_by(getattr(Table, 'town')).filter(
   Table.pop2010 > 40000).all()[0].town
# ST_Intersects(ST_SetSRID(ST_MakePoint(-72, 42.3601), 4326),
# ST_Transform(geom, 4326))
"""

import sqlalchemy
import sqlalchemy.orm
import time

# from six.moves import range

from girder import logger as log

from . import base


PostgresOperators = {
    'eq': '=',
    'ne': '!=',
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
    'regex': '~',
    'not_regex': '!~',
    'search': '~*',  # This could be custom
    'not_search': '!~*',  # This could be custom
}


class PostgresSAConnector(base.DatabaseConnector):
    name = 'postgres_sqlalchemy'

    def __init__(self, *args, **kwargs):
        super(PostgresSAConnector, self).__init__(*args, **kwargs)
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

        # Additional parameters:
        #  idletime: seconds after which a connection is considered idle
        #  abandontime: second after which a connection will be abandoned
        self.dbIdleTime = float(kwargs.get('idletime', 300))
        self.dbAbandonTime = float(kwargs.get('abandontime',
                                   self.dbIdleTime * 5))
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

    def _applyFilter(self, query, filter):
        """
        Apply a filter to a SQLAlchemy session query.

        :param query: the SQLAlchemy session query.
        :param filter: information on the filter.
        :return: a filtered session query.
        """
        operator = filter['operator']
        operator = base.FilterOperators.get(operator, operator)
        operator = PostgresOperators.get(operator, operator)
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
            return fieldOrFunction['value']
        fieldOrFunction = self.isFunction(fieldOrFunction)
        if fieldOrFunction is False:
            raise Exception('Not a function')
        return getattr(sqlalchemy.func, fieldOrFunction['func'])(
            *[self._convertFieldOrFunction(entry, True) for entry in
              fieldOrFunction['param']])

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
            self.dbEngine = sqlalchemy.create_engine(
                self.databaseUrl, **self.dbparams)
            metadata = sqlalchemy.MetaData(self.dbEngine)
            table = sqlalchemy.Table(self.table, metadata, schema=self.schema,
                                     autoload=True)
            sqlalchemy.orm.mapper(self.tableClass, table)
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
                           and sort.
        :param filters: a list of filters to apply.
        :param client: if a client is specified, a previous query made by this
                       client can be cancelled.
        :return: the results of the query.  See above.
        """
        if queryProps.get('fields') is None:
            if not fields:
                fields = self.getFieldInfo()
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
        # filters
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
        log.info('Query: %s', query.statement)
        result['data'] = list(query.values(*[
            self._convertFieldOrFunction(field)
            for field in queryProps['fields']]))
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


base.registerConnectorClass(PostgresSAConnector.name, PostgresSAConnector)

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

import psycopg2
import psycopg2.errorcodes
import six
import threading
import time

from six.moves import range

from girder import logger as log

from . import base


# Per distinct database.  Should be less than 90% of available connections to
# postgres based on its config between all instances of the app that are
# running.
PostgresPoolSize = 10

PostgresOperators = {
    'eq': '=',
    'ne': '!=',
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
    'regex': '~',
    'notregex': '!~',
    'search': '~*',  # This could be custom
    'notsearch': '!~*',  # This could be custom
}


def escapeName(name, schema=None, addSchema=True, allowInternalSchema=False):
    """
    Postgres table and field names could have any characters.  To prevent
    injection attacks, escape and quote names in U&"(name)" format, which
    prevents the name from performing other sql or using reserved words.  In
    theory, if we knew that a name was not a reserved postgres or sql word nor
    a built-in table, we could skip quoting it.  In practice, it is safer to
    always use quotes.

    :param name: name to escape and quote.
    :return: the quoted name.
    """
    safe = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_'
    name = name.decode('utf8')
    escname = []
    for k in name:
        if k not in safe:
            kord = ord(k)
            if kord > 0xFFFF:
                escname.append('\\+%06X' % kord)
            else:
                escname.append('\\%04X' % kord)
        else:
            escname.append(k)
    escname = ''.join(escname)
    if '\\' in escname:
        escname = 'U&"' + escname + '"'
    else:
        escname = '"' + escname + '"'
    return escname


def escapeTableNameWithSchema(name, schema=None, addSchema=True,
                              allowInternalSchema=False):
    """
    Escape a table name with a schema.  If no schema is specified, try to parse
    parse the schema from the table name.  If no schema is present, optionally
    add a schema.

    :param name: name to escape and quote.
    :param schema: if present, the schema to use.
    :param addSchema: if True and no schema is specified or in the table name,
                      add the 'public' schema.
    :param allowInternalSchema: if False, disallow any schema that starts with
            pg_ or equals information_schema.  Not that if no schema is present
            and addSchema is False, internal tables can still be reached.
    :return: the quoted schema and name.  None if the name or schema was
             disallowed.
    """
    if name is not None and not isinstance(name, six.string_types):
        name = str(name)
    if name is None or not len(name):
        return None
    if schema is not None and not isinstance(schema, six.string_types):
        schema = str(schema)
    if schema is not None and not len(schema):
        schema = None
    if schema is None and '.' in name[1:-1]:
        pos = name.index('.', 1)
        schema = name[:pos]
        name = name[pos + 1:]
    if addSchema and schema is None:
        schema = 'public'
    if (not allowInternalSchema and schema is not None and (
            schema.startswith('pg_') or schema == 'information_schema')):
        return None
    if schema is not None:
        return escapeName(schema) + '.' + escapeName(name)
    return escapeName(name)


class PostgresConnector(base.DatabaseConnector):
    name = 'postgres'

    def __init__(self, *args, **kwargs):
        super(PostgresConnector, self).__init__(*args, **kwargs)
        if not self.validate(**kwargs):
            return
        self.table = escapeTableNameWithSchema(kwargs.get('table'),
                                               kwargs.get('schema'))
        # dbparams can include
        #  dsn, database, user, password, host, port
        #  values in: http://www.postgresql.org/docs/current/static/
        #             libpq-connect.html#LIBPQ-PARAMKEYWORDS
        self.dbparams = {}
        for key in kwargs:
            # This is an exclusion list, since there are a lot of parameters
            # available for the postgres connection
            if key not in ('table', 'idletime', 'abandontime', 'type',
                           'schema'):
                self.dbparams[key] = kwargs[key]
        self.dbLock = threading.RLock()
        self.dbpool = []
        self.maxPoolSize = PostgresPoolSize
        # Additional parameters:
        #  idletime: seconds after which a connection is considered idle
        #  abandontime: second after which a connection will be abandoned
        self.dbIdleTime = float(kwargs.get('idletime', 300))
        self.dbAbandonTime = float(kwargs.get('abandontime',
                                   self.dbIdleTime * 5))
        self.stopped = False
        self.closeThread = threading.Thread(target=self.closeWhenIdle)
        self.closeThread.daemon = True
        self.closeThread.start()
        self.fields = None
        self.initialized = True

    def __del__(self):
        """
        Mark this instance as stopped.  This will let the idle thread close
        gracefully.
        """
        self.stopped = True

    def _convertFilter(self, filter):
        """
        Convert a filter into the a postres where cluase.

        :param filter: information on the filter.
        :return: an array to add to the sql query and an array to add to the
                 escaped sql values.
        """
        operator = filter['operator']
        operator = base.FilterOperators.get(operator, operator)
        operator = PostgresOperators.get(operator, operator)
        if operator in ('in', 'notin'):
            values = filter['value']
            if not isinstance(values, (list, tuple)):
                values = [values]
            sql = [' AND ', escapeName(filter['field']),
                   '' if operator == 'in' else ' NOT', ' IN (',
                   ','.join(['%s' for value in values]), ')']
            sqlvalues = values
            return sql, sqlvalues
        sql = [' AND ', escapeName(filter['field']), ' ', operator, ' %s']
        sqlvalues = [filter['value']]
        return sql, sqlvalues

    def _performQuery(self, sql, sqlvalues=[], client=None, maxRetries=3):
        """
        Perform a query on a database with retries and gaurds against
        cancelling.

        :param sql: the query to perform.  Values that should be escaped are
                    passed in the sqlvalues array and are represented as %s in
                    this query string.
        :param sqlvalues: a list or tuple of values to escape and place in the
                          query.
        :param client: the connection pool client.
        :param maxRetries: the number of times to attempt a connection.
        :returns: db: a database reference.
                  c: a database cursor where the query has been performed.
        """
        for retry in range(maxRetries):
            db = None
            try:
                db = self.connect(retry != 0, client=client)
                c = db.cursor()
                log.info('Query: %s', c.mogrify(sql, sqlvalues))
                c.execute(sql, sqlvalues)
                return db, c
            except psycopg2.Error as exc:
                if db:
                    self.disconnect(db, client)
                try:
                    code = psycopg2.errorcodes.lookup(exc.pgcode)
                except KeyError:
                    code = '%s' % exc.pgcode
                log.info('Database error %s - %s', str(exc).strip(), code)
                if code == 'QUERY_CANCELED':
                    return None, None
        return None, None

    def closeWhenIdle(self):
        """
        Periodically check if the database has been used.  If not, close the
        connection to free resources and allow easier management of the
        database while the application is running.
        """
        while not self.stopped:
            with self.dbLock:
                curtime = time.time()
                for pos in range(len(self.dbpool) - 1, -1, -1):
                    delta = curtime - self.dbpool[pos]['time']
                    if ((not self.dbpool[pos]['used'] and
                            delta > self.dbIdleTime) or
                            delta > self.dbAbandonTime):
                        # The old db connection will close when no process is
                        # using it
                        self.dbpool[pos:pos + 1] = []
            time.sleep(30)

    def connect(self, reconnect=False, client=None):
        """
        Connect to the database.

        :param reconnect: if False, allow an open connection to be returned.
                          If 'fresh', create a new connection that the caller
                          is responsible for closing that isn't part of the
                          pool.  The client is ignored in this case.  If True,
                          close any existing connections that aren't in use or
                          are for this client, and return a new connection.
        :param client: if None, use the next connection in the pool.  If
                       specified, if this client is currently marked in use,
                       cancel the client's existing query and return a
                       connection from the pool for the client to use.
        :return: a database object.
        """
        if reconnect == 'fresh':
            return psycopg2.connect(**self.dbparams)
        db = None
        with self.dbLock:
            if client:
                for pos in range(len(self.dbpool) - 1, -1, -1):
                    if self.dbpool[pos].get('client', None) == client:
                        self.dbpool[pos]['db'].cancel()
                        if reconnect:
                            self.dbpool[pos:pos + 1] = []
                        else:
                            self.dbpool[pos]['used'] = False
                            self.dbpool[pos]['client'] = None
            if reconnect:
                if len(self.dbpool) >= self.maxPoolSize:
                    for pos in range(len(self.dbpool)):
                        if not self.dbpool[pos]['used']:
                            self.dbpool[pos]['db'].close()
                            self.dbpool[pos:pos + 1] = []
                            break
            if not reconnect:
                for pos in range(len(self.dbpool)):
                    if not self.dbpool[pos]['used']:
                        db = self.dbpool[pos]['db']
                        self.dbpool[pos]['used'] = True
                        self.dbpool[pos]['client'] = client
                        self.dbpool[pos]['time'] = time.time()
                        break
            if not db:
                db = psycopg2.connect(**self.dbparams)
                self.dbpool.append({
                    'db': db,
                    'used': True,
                    'client': client,
                    'time': time.time()
                })
        # This is equivalent to "set default_transaction_read_only=on;".  It
        # isn't a real guard against change, as if we could allow an injection
        # attack, it could be turned off.  Also, volatile functions can still
        # have side effects (for instance, setseed() changes the state for
        # generating random numbers which could have cryptographic
        # implications).
        db.set_session(readlonly=True)
        return db

    def disconnect(self, db, client=None):
        """
        Mark that a client has finished with a database connection and it can
        be closed or returned to the pool.

        :param db: the database connection to mark as finished.
        :param client: the client that owned this connection.
        """
        with self.dbLock:
            for pos in range(len(self.dbpool)):
                if self.dbpool[pos]['db'] == db:
                    self.dbpool[pos]['used'] = False
                    self.dbpool[pos]['client'] = None
                    if len(self.dbpool) > self.maxPoolSize:
                        self.dbpool[pos:pos + 1] = []
                    break

    def getFieldInfo(self):
        """
        Return a list of fields that are known and can be queried.

        :return: a list of known fields.  Each entry is a dictionary with name,
                 datatype, and optionally a description.
        """
        if self.fields is not None:
            return self.fields
        db = self.connect()
        c = db.cursor()
        fields = []
        types = set()
        try:
            c.execute('SELECT * FROM %s LIMIT 0;' % self.table)
            for column in c.description:
                fields.append({
                    'name': column[0],
                    'typeid': column[1],
                })
                types.add(column[1])
        except psycopg2.Error:
            log.exception('Cannot determine field information')
        if len(types):
            # A partial category map from Postgres
            categoryMap = {
                'A': 'array',
                'B': 'boolean',
                'D': 'date',
                'E': 'enum',
                'G': 'geometric',
                'I': 'netaddr',
                'N': 'number',
                'S': 'string',
                'T': 'duration',
            }
            pg_types = {}
            # This gets the list of known types from Postgres.  psycopg2
            # returns the pg_type.oid of each column, so it can be matched with
            # these values.  The typcategory, broadly, determines what features
            # we want to expose per column.
            c.execute('SELECT pg_type.oid, typname, typtype, typcategory from '
                      'pg_catalog.pg_type WHERE pg_type.oid in (%s);' % (
                          ','.join([str(id) for id in types])))
            for row in c:
                pg_types[row[0]] = {
                    'name': row[1],
                    'type': row[2],
                    'category': categoryMap.get(row[3], row[3])
                }
            for field in fields:
                if field['typeid'] in pg_types:
                    field['type'] = pg_types[field['typeid']]['category']
                    field['typename'] = pg_types[field['typeid']]['name']
        c.close()
        self.disconnect(db)
        # ##DWM:: If our specification includes field information, add
        # descriptions.  Also, the endpoint that calls this should filter the
        # data to just name and type if not a SITE_ADMIN.
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
        sql = ['SELECT ' + ','.join([
            escapeName(field) for field in queryProps['fields']]) +
            ' FROM ' + self.table + ' WHERE TRUE']
        sqlvalues = []
        for filter in filters:
            subsql, subsqlvalues = self._convertFilter(filter)
            sql.extend(subsql)
            sqlvalues.extend(subsqlvalues)
        if queryProps.get('sort'):
            for pos in range(len(queryProps['sort'])):
                sort = queryProps['sort'][pos]
                sql.append(' ORDER BY' if not pos else ',')
                sql.append(' ' + escapeName(sort[0]) +
                           (' ASC' if sort[1] != -1 else ' DESC'))
        if 'limit' in queryProps:
            sql.append(' LIMIT %d' % queryProps['limit'])
        if 'offset' in queryProps:
            sql.append(' OFFSET %d' % queryProps['offset'])
        sql = ''.join(sql)
        db, c = self._performQuery(sql, sqlvalues, client)
        if db is None:
            return
        try:
            result['data'] = data = c.fetchmany()
            while data:
                data = c.fetchmany()
                if data:
                    result['data'].extend(data)
            c.close()
        except psycopg2.Error as exc:
            code = psycopg2.errorcodes.lookup(exc.pgcode)
            log.info('Database error %s - %s', str(exc).strip(), code)
        self.disconnect(db, client=client)
        return result

    @staticmethod
    def validate(table=None, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        table = escapeTableNameWithSchema(table, kwargs.get('schema'))
        if not table:
            return False
        # We could validate other database parameters, too
        return True


base.registerConnectorClass(PostgresConnector.name, PostgresConnector)

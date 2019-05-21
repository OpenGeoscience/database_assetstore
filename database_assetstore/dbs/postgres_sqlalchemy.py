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

import re
import six
import sqlalchemy
import sqlalchemy.dialects.postgresql as dialect

from girder import logger as log

from . import base
from .sqlalchemydb import SQLAlchemyConnector


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

PostgresDatatypes = {
    'number': ('integer', 'bigint', 'int8', 'bigserial', 'numeric',
               'double precision', 'float8', 'int', 'int4', 'real', 'float4',
               'smallint', 'int2', 'smallserial', 'serial2', 'serial',
               'serial4'),
    'boolean': ('boolean', 'bool'),
    'string': ('character', 'char', 'character varying', 'varchar', 'json',
               'text'),
    'date': ('timestamp', 'timestamp with time zone',
             'timestamp without time zone'),
}


KnownTypes = {}


class PostgresSAConnector(SQLAlchemyConnector):
    name = 'sqlalchemy_postgres'

    def __init__(self, *args, **kwargs):
        # The super class also validates the connector
        super(PostgresSAConnector, self).__init__(*args, **kwargs)
        # dbparams can include values in http://www.postgresql.org/docs/
        #   current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        self.databaseOperators = PostgresOperators
        # Get a list of types and their classes so that we can cast using
        # sqlalchemy
        self.types = KnownTypes
        KnownTypes.update({
            type: getattr(dialect, type) for type in dir(dialect)
            if isinstance(getattr(dialect, type),
                          sqlalchemy.sql.visitors.VisitableType)})
        # Include types that were added to the ischema_names table (this is
        # done, for instance, by the geoalchemy module).
        for ikey, itype in six.iteritems(dialect.base.ischema_names):
            key = getattr(itype, '__visit_name__', None)
            if key and key not in KnownTypes:
                KnownTypes[key] = itype

    def _isFunctionAllowed(self, funcname):
        """
        Check if the specified function is allowed.  Currently, only
        non-volatile functions are allowed, even though there are volatile
        functions that are harmless.  We also prohibit pg_* and _* functions,
        since those are likely to be internal functions.

        :param funcname: name of the function to check.
        :returns: True is allowed, False is not.
        """
        if not getattr(self, '_checkedAllowedFunctions', False):
            setattr(self, '_checkedAllowedFunctions', True)
            db = self.connect()
            funcs = db.execute(
                'SELECT lower(proname), provolatile FROM pg_proc;').fetchall()
            self.disconnect(db)
            self._allowedFunctions = {
                func[0]: (func[1] in ('i', 's') and
                          not func[0].startswith('pg_') and
                          not func[0].startswith('_'))
                for func in funcs}
            # If any variant of a function is volatile, don't allow it (a
            # function can be overloaded for different data types)
            for func in funcs:
                if (func[1] not in ('i', 's') and
                        self._allowedFunctions.get(func[0], False)):
                    self._allowedFunctions[func[0]] = False
            self._allowedFunctions['cast'] = True
            self._allowedFunctions['count'] = True
            self._allowedFunctions['distinct'] = True
        return self._allowedFunctions.get(funcname.lower(), False)

    def setSessionReadOnly(self, sess):
        """
        Set the specified session to read only if possible.  Subclasses should
        implement the appropriate behavior.

        :param sess: the session to adjust.
        """
        try:
            sess.execute('set default_transaction_read_only=on')
        except sqlalchemy.exc.OperationalError:
            log.warn('Couldn\'t set default_transaction to read_only')
            sess.rollback()

    def getFieldInfo(self):
        """
        Return a list of fields that are known and can be queried.

        :return: a list of known fields.  Each entry is a dictionary with name,
                 datatype, and optionally a description.
        """
        if self.fields is not None:
            return self.fields
        super(PostgresSAConnector, self).getFieldInfo()
        if self.fields is not None:
            for field in self.fields:
                if 'type' in field:
                    datatype = field['type'].lower().split('(')[0]
                    for key in PostgresDatatypes:
                        if datatype in PostgresDatatypes[key]:
                            datatype = key
                            break
                    field['datatype'] = datatype
        return self.fields


# Not all types in Postgres are known to SQLAlchemy.  We want to report this
# information so that consumers of the data know what types are involved.  For
# any type that is unknown, we create a UserDefinedType and then add it to our
# list of KnownTypes so that casts can be done to this type if so desired.

class DynamicType(sqlalchemy.types.UserDefinedType):
    def __init__(self, *args, **kwargs):
        """
        The __init__ function needs to exist or an error is thrown, but we
        don't need to do anything.  When a type is created dynamically, it will
        have a class attribute of "name" which contains the type name.
        """
        pass

    def get_col_spec(self, **kw):
        """
        When we are asked to generate a column specification, use the original
        type name.
        """
        return self.name

    def bind_processor(self, dialect):
        def process(value):
            return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value
        return process


# We repalce the internal Postgres dialect's _get_column_info so we can add
# type classes as necessary.  It would be nicer to not have to monkey-patch
# the module, but it doesn't expose the unknown types anywhere else; otherwise,
# we would have to repeat the database introspection.
gci = dialect.base.PGDialect._get_column_info


def _get_column_info(self, name, format_type, *args, **kwargs):
    """
    When the PGDialect or its subclasses get column information, if the type
    is unknown and certain conditions are met, create a new type dynamically.
    This wraps the original function (see sqlalchemy's
    dialects/postgresql/base.py file).
    """
    attype = re.sub(r'\(.*\)', '', format_type)
    attype = re.sub(r'\[\]', '', attype)
    info = gci(self, name, format_type, *args, **kwargs)
    if (info['type'] == sqlalchemy.sql.sqltypes.NULLTYPE and
            attype.lower() not in dialect.base.ischema_names and
            attype not in KnownTypes):
        newtype = type(str(attype), (DynamicType,), {'name': str(attype)})
        newtype.__visit_name__ = attype
        dialect.base.ischema_names[attype.lower()] = newtype
        KnownTypes[attype] = newtype
        info = gci(self, name, format_type, *args, **kwargs)
    return info


dialect.base.PGDialect._get_column_info = _get_column_info


base.registerConnectorClass(PostgresSAConnector.name, PostgresSAConnector, {
    'dialects': {
        'postgresql': 'postgresql',
        'postgres': 'postgresql',
        'psql': 'postgresql',
        'pgdb': 'postgresql'
    },
    'default_dialect': 'postgresql',
    'priority': 0,
})

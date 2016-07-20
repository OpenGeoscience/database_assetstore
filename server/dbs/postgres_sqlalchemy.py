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


class PostgresSAConnector(SQLAlchemyConnector):
    name = 'sqlalchemy_postgres'

    def __init__(self, *args, **kwargs):
        super(PostgresSAConnector, self).__init__(*args, **kwargs)
        if not self.validate(**kwargs):
            return
        # dbparams can include values in http://www.postgresql.org/docs/
        #   current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        self.databaseOperators = PostgresOperators

    def _isFunctionAllowed(self, funcname):
        """
        Check if the specified function is allowed.  Currently, only
        non-volatile functions are allowed, even though there are volatile
        functions that are harmless.  We also prohibit pg_* and _* functions,
        since those are likely to be internal functions.

        :param funcname: name of the function to check.
        :returns: True is allowed, False is not.
        """
        if not self._allowedFunctions or not len(self._allowedFunctions):
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
            # Whitelist some functions if they exist
            for func in funcs:
                if func[0] in ('pg_sleep', ):
                    self._allowedFunctions[func[0]] = True
        return self._allowedFunctions.get(funcname.lower(), False)

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

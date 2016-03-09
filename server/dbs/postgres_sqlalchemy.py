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
        return self._allowedFunctions.get(funcname.lower(), False)


base.registerConnectorClass(PostgresSAConnector.name, PostgresSAConnector)

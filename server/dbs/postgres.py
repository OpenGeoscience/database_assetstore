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


class PostgresConnector(base.DatabaseConnector):
    name = 'postgres'

    def __init__(self, *args, **kwargs):
        super(PostgresConnector, self).__init__(*args, **kwargs)

    @staticmethod
    def validate(table=None, **kwargs):
        """
        Validate that the passed arguments are sufficient for connecting to the
        database.

        :returns: True if the arguments should allow connecting to the db.
        """
        if table is None:
            return False
        # We could validate a uri
        return True


base._connectorClasses[PostgresConnector.name] = PostgresConnector

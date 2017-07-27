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

from .base import (
    getDBConnectorClass, getDBConnector, getDBConnectorClassFromDialect,
    clearDBConnectorCache, FilterOperators, DatabaseConnectorException,
    databaseFromUri, DatabaseConnector)
from . import sqlalchemydb
from . import mysql_sqlalchemy
from . import postgres_sqlalchemy
from . import mongo

__all__ = [
    'getDBConnectorClass', 'getDBConnector', 'getDBConnectorClassFromDialect',
    'clearDBConnectorCache', 'FilterOperators', 'DatabaseConnectorException',
    'databaseFromUri', 'DatabaseConnector', 'sqlalchemydb', 'mysql_sqlalchemy',
    'postgres_sqlalchemy', 'mongo',
]

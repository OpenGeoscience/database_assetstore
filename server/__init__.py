#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
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
#############################################################################

from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AccessType, AssetstoreType
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from girder.utility.model_importer import ModelImporter

from .assetstore import DatabaseAssetstoreAdapter
from .rest import DatabaseItemResource, dbInfoKey, DatabaseAssetstoreResource


def updateAssetstore(event):
    params = event.info['params']
    assetstore = event.info['assetstore']

    if assetstore['type'] == AssetstoreType.DATABASE:
        assetstore['database'] = {
            'dbtype': params.get('dbtype', assetstore['database']['dbtype']),
            'uri': params.get('dburi', assetstore['database']['uri'])
        }


@access.admin
def createAssetstore(event):
    params = event.info['params']

    if params.get('type') == AssetstoreType.DATABASE:
        event.addResponse(ModelImporter.model('assetstore').save({
            'type': AssetstoreType.DATABASE,
            'name': params.get('name'),
            'database': {
                'dbtype': params.get('dbtype'),
                'uri': params.get('dburi'),
            }
        }))
        event.preventDefault()


def load(info):
    AssetstoreType.DATABASE = 'database'
    setAssetstoreAdapter(AssetstoreType.DATABASE, DatabaseAssetstoreAdapter)
    events.bind('assetstore.update', 'database_assetstore', updateAssetstore)
    events.bind('rest.post.assetstore.before', 'database_assetstore',
                createAssetstore)

    (Assetstore.createAssetstore.description
        .param('dbtype', 'The database type (for Database type).',
               required=False)
        .param('dburi', 'The database URI (for Database type).',
               required=False))

    info['apiRoot'].database_assetstore = DatabaseAssetstoreResource()

    DatabaseItemResource(info['apiRoot'])

    ModelImporter.model('item').exposeFields(
        level=AccessType.ADMIN, fields=dbInfoKey)
    ModelImporter.model('item').exposeFields(
        level=AccessType.SITE_ADMIN, fields=dbInfoKey)

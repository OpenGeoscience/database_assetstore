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

import functools

from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore as AssetstoreResource
from girder.constants import AccessType, AssetstoreType
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.utility.assetstore_utilities import setAssetstoreAdapter

from . import assetstore
from . import base
from .rest import DatabaseAssetstoreResource, fileResourceRoutes


@access.admin
def createAssetstore(event):
    """
    When an assetstore is created, make sure it has a well-formed database
    information record.

    :param event: Girder rest.post.assetstore.before event.
    """
    params = event.info['params']

    if params.get('type') == AssetstoreType.DATABASE:
        event.addResponse(Assetstore().save({
            'type': AssetstoreType.DATABASE,
            'name': params.get('name'),
            'database': {
                'dbtype': params.get('dbtype'),
                'uri': params.get('dburi'),
            }
        }))
        event.preventDefault()


def updateAssetstore(event):
    """
    When an assetstore is updated, make sure the result has a well-formed set
    of database information.

    :param event: Girder assetstore.update event.
    """
    params = event.info['params']
    store = event.info['assetstore']

    if store['type'] == AssetstoreType.DATABASE:
        dbtype = params.get('dbtype', store['database']['dbtype'])
        if dbtype == assetstore.DB_ASSETSTORE_USER_TYPE:
            store['database'] = {
                'dbtype': dbtype
            }
        else:
            store['database'] = {
                'dbtype': dbtype,
                'uri': params.get('dburi', store['database']['uri'])
            }


def validateFile(event):
    """
    When a file is being validated, let the database assetstore check the
    validation.

    :param event: the validation event.  info is the file document.
    """
    assetstore.validateFile(event.info)


def load(info):
    """
    Load the plugin into Girder.

    :param info: a dictionary of plugin information.  The name key contains the
                 name of the plugin according to Girder.
    """
    plugin_name = info['name']
    AssetstoreType.DATABASE = 'database'
    setAssetstoreAdapter(AssetstoreType.DATABASE,
                         assetstore.DatabaseAssetstoreAdapter)
    events.bind('assetstore.update', 'database_assetstore', updateAssetstore)
    events.bind('rest.post.assetstore.before', 'database_assetstore',
                createAssetstore)
    events.bind('model.file.validate', 'database_assetstore', validateFile)
    events.bind('model.setting.validate', 'database_assetstore',
                functools.partial(base.validateSettings, plugin_name=plugin_name))

    (AssetstoreResource.createAssetstore.description
        .param('dbtype', 'The database type (for Database type).',
               required=False)
        .param('dburi', 'The database URI (for Database type).',
               required=False))

    info['apiRoot'].database_assetstore = DatabaseAssetstoreResource()

    fileResourceRoutes(info['apiRoot'].file)

    File().exposeFields(level=AccessType.ADMIN, fields=base.DB_INFO_KEY)
    File().exposeFields(level=AccessType.SITE_ADMIN, fields=base.DB_INFO_KEY)

    # Make sure the user assetstore exists.
    base._createUserAssetstore()

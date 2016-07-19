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
from girder.api.v1.assetstore import Assetstore
from girder.constants import AccessType, AssetstoreType, SettingKey
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from girder.utility.model_importer import ModelImporter

from . import assetstore
from .rest import DatabaseFileResource, dbInfoKey, DatabaseAssetstoreResource


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


def updateAssetstore(event):
    params = event.info['params']
    assetstore = event.info['assetstore']

    if assetstore['type'] == AssetstoreType.DATABASE:
        assetstore['database'] = {
            'dbtype': params.get('dbtype', assetstore['database']['dbtype']),
            'uri': params.get('dburi', assetstore['database']['uri'])
        }


def validateFile(event):
    """
    When a file is being validated, let the database assetstore check the
    validation.

    :param event: the validation event.  info is the file document.
    """
    assetstore.validateFile(event.info)


def validateSettings(event, plugin_name=None):
    """
    Validate plugin-specific settings and prevent disabling this plugin if
    there are any files in database assetstores.

    :param plugin_name: the name of our plugin.
    :param event: the validation event
    """
    key, val = event.info['key'], event.info['value']

    # If we are validating the list of enabled plugins, and there are any
    # database assetstores with files, do not allow the plugin to be disabled.
    if (key == SettingKey.PLUGINS_ENABLED and plugin_name and
            plugin_name not in val):
        if any(store['type'] == AssetstoreType.DATABASE and store['hasFiles']
               for store in ModelImporter.model('assetstore').list()):
            val.append(plugin_name)


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
                functools.partial(validateSettings, plugin_name=plugin_name))

    (Assetstore.createAssetstore.description
        .param('dbtype', 'The database type (for Database type).',
               required=False)
        .param('dburi', 'The database URI (for Database type).',
               required=False))

    info['apiRoot'].database_assetstore = DatabaseAssetstoreResource()

    DatabaseFileResource(info['apiRoot'])

    ModelImporter.model('file').exposeFields(
        level=AccessType.ADMIN, fields=dbInfoKey)
    ModelImporter.model('file').exposeFields(
        level=AccessType.SITE_ADMIN, fields=dbInfoKey)

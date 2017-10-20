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
import jsonschema

from girder import events
from girder import logger as log
from girder.api import access
from girder.api.v1.assetstore import Assetstore as AssetstoreResource
from girder.constants import AccessType, AssetstoreType, SettingKey
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.models.model_base import ValidationException
from girder.utility import setting_utilities
from girder.utility.assetstore_utilities import setAssetstoreAdapter

from . import assetstore
from .rest import DB_INFO_KEY, DatabaseAssetstoreResource, fileResourceRoutes


_userDatabaseGroupsSchema = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'pattern': {
                'type': 'string'
            },
            'groupId': {
                'type': 'string',
                'pattern': '^(|[0-9a-fA-F]{24})$'
            }
        },
        'required': ['pattern', 'groupId']
    }
}


class PluginSettings(object):
    USER_DATABASES = 'database_assetstore.user_databases'
    USER_DATABASES_GROUPS = 'database_assetstore.user_databases_groups'


@setting_utilities.validator(PluginSettings.USER_DATABASES)
def _validateBoolean(doc):
    if not isinstance(doc['value'], bool):
        raise ValidationException('%s setting must be true or false.' % doc['key'])


@setting_utilities.validator(PluginSettings.USER_DATABASES_GROUPS)
def _validateGroup(doc):
    if doc['value']:
        try:
            jsonschema.validate(doc['value'], _userDatabaseGroupsSchema)
        except jsonschema.ValidationError as e:
            raise ValidationException('Invalid user database groups rules: ' + e.message)


def _createUserAssetstore():
    """
    Add a general user assetstore if it doesn't exist.  This uses a fixed ID so
    that if it is deleted and the plugin starts again, existing db assets will
    still work.
    """
    if not Assetstore().load(assetstore.DB_ASSETSTORE_ID):
        Assetstore().save({
            '_id': assetstore.DB_ASSETSTORE_ObjectId,
            'type': AssetstoreType.DATABASE,
            'name': assetstore.DB_ASSETSTORE_USER_NAME,
            'database': {
                'dbtype': assetstore.DB_ASSETSTORE_USER_TYPE
            }
        })


def _removeUserAssetstore():
    """
    Remove the user assetstore if it exists.
    """
    store = Assetstore().load(assetstore.DB_ASSETSTORE_ID)
    if store:
        Assetstore().remove(store)


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
        store = next((store for store in Assetstore().list()
                      if store['type'] == AssetstoreType.DATABASE and
                      store['hasFiles']), None)
        if store:
            val.append(plugin_name)
            log.info('Won\'t disable %s because there are files in the %s assetstore' % (
                plugin_name, store['name']))
    if (key == SettingKey.PLUGINS_ENABLED and plugin_name):
        if plugin_name not in val:
            _removeUserAssetstore()
        else:
            _createUserAssetstore()


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

    (AssetstoreResource.createAssetstore.description
        .param('dbtype', 'The database type (for Database type).',
               required=False)
        .param('dburi', 'The database URI (for Database type).',
               required=False))

    info['apiRoot'].database_assetstore = DatabaseAssetstoreResource()

    fileResourceRoutes(info['apiRoot'].file)

    File().exposeFields(level=AccessType.ADMIN, fields=DB_INFO_KEY)
    File().exposeFields(level=AccessType.SITE_ADMIN, fields=DB_INFO_KEY)

    # Make sure the user assetstore exists.
    _createUserAssetstore()

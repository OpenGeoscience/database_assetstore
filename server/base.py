from bson.objectid import ObjectId
import jsonschema

from girder import logger as log
from girder.constants import AssetstoreType, SettingKey
from girder.exceptions import ValidationException
from girder.models.assetstore import Assetstore
from girder.utility import setting_utilities


DB_INFO_KEY = 'databaseMetadata'

# This is a quasi-random ObjectID with an 'old' date
DB_ASSETSTORE_ID = '4b3d3b0193371b154c68f931'
DB_ASSETSTORE_ObjectId = ObjectId(DB_ASSETSTORE_ID)
DB_ASSETSTORE_USER_NAME = 'User-authorized Database Assetstore'
DB_ASSETSTORE_USER_TYPE = 'USER'


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
    if not Assetstore().load(DB_ASSETSTORE_ID):
        Assetstore().save({
            '_id': DB_ASSETSTORE_ObjectId,
            'type': AssetstoreType.DATABASE,
            'name': DB_ASSETSTORE_USER_NAME,
            'database': {
                'dbtype': DB_ASSETSTORE_USER_TYPE
            }
        })


def _removeUserAssetstore():
    """
    Remove the user assetstore if it exists.
    """
    store = Assetstore().load(DB_ASSETSTORE_ID)
    if store:
        Assetstore().remove(store)


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

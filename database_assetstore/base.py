from bson.objectid import ObjectId
import jsonschema

from girder import logger as log
from girder.constants import AssetstoreType
from girder.exceptions import ValidationException
from girder.models.assetstore import Assetstore
from girder.utility import setting_utilities, toBool


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
    doc['value'] = toBool(doc['value'])


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
    if not Assetstore().load(DB_ASSETSTORE_ObjectId):
        Assetstore().save({
            '_id': DB_ASSETSTORE_ObjectId,
            'type': AssetstoreType.DATABASE,
            'name': DB_ASSETSTORE_USER_NAME,
            'database': {
                'dbtype': DB_ASSETSTORE_USER_TYPE
            }
        })

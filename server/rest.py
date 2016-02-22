#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
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
###############################################################################

import datetime
import six

from girder.api import access
from girder.api.v1.item import Item
from girder.api.describe import describeRoute, Description
from girder.api.rest import filtermodel, loadmodel, RestException
from girder.models.model_base import AccessType

from .dbs import getDBConnectorClass


dbInfoKey = 'databaseMetadata'


class DatabaseItemResource(Item):

    def __init__(self, apiRoot):
        # Don't call the parent (Item) constructor, to avoid redefining routes,
        # but do call the grandparent (Resource) constructor
        super(Item, self).__init__()

        self.resourceName = 'item'
        apiRoot.item.route('GET', (':id', 'database'), self.getDatabaseLink)
        apiRoot.item.route('POST', (':id', 'database'),
                           self.createDatabaseLink)
        apiRoot.item.route('DELETE', (':id', 'database'),
                           self.deleteDatabaseLink)
        # apiRoot.item.route('GET', (':id', 'database', 'fields'),
        #                    self.getDatabaseFields)
        # apiRoot.item.route('GET', (':id', 'database', 'select'),
        #                    self.getDatabaseSelect)

    @describeRoute(
        Description('Get item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.READ)
    @filtermodel(model='item')
    def getDatabaseLink(self, item, params):
        return item.get(dbInfoKey)

    @describeRoute(
        Description('Set or modify item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
        .param('body', 'A JSON object containing the database information to '
               'update. At a minimum this must include "type", "uri", and '
               '"table".', paramType='body')
        .notes('Set database information fields tp null to delete them.')
        .errorResponse('ID was invalid.')
        .errorResponse('Invalid JSON passed in request body.')
        .errorResponse('Unknown database type.')
        .errorResponse('Database information is invalid.')
        .errorResponse('Write access was denied for the item.', 403)
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.ADMIN)
    @filtermodel(model='item')
    def createDatabaseLink(self, item, params):
        dbinfo = self.getBodyJson()
        if dbInfoKey not in item:
            item[dbInfoKey] = {}
        item[dbInfoKey].update(six.viewitems(dbinfo))
        toDelete = [k for k, v in six.viewitems(item[dbInfoKey]) if v is None]
        for key in toDelete:
            del item[dbInfoKey][key]
        item['updated'] = datetime.datetime.utcnow()
        dbinfo = item[dbInfoKey]
        # Generate type set from connector classes
        connClass = getDBConnectorClass(dbinfo.get('type'))
        if not connClass:
            raise RestException('Unknown database type.')
        if not connClass.validate(**dbinfo):
            raise RestException('Database information is invalid.')
        # ##DWM:: check if the database is reachable, and warn if it isn't?
        return self.model('item').save(item)

    @describeRoute(
        Description('Delete item database link information.')
        .param('id', 'The ID of the item.', paramType='path')
    )
    @access.user
    @loadmodel(model='item', map={'id': 'item'}, level=AccessType.WRITE)
    def deleteDatabaseLink(self, item, params):
        deleted = False
        if dbInfoKey in item:
            del item[dbInfoKey]
            self.model('item').save(item)
            deleted = True
        return {
            'deleted': deleted
        }

    """
    @describeRoute(
        Description('Get a large image tile.')
        .param('id', 'The ID of the item or "test".', paramType='path')
        .param('z', 'The layer number of the tile (0 is the most zoomed-out '
               'layer).', paramType='path')
        .param('x', 'The X coordinate of the tile (0 is the left side).',
               paramType='path')
        .param('y', 'The Y coordinate of the tile (0 is the top).',
               paramType='path')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
    )
    @access.cookie
    @access.public
    def getTile(self, id, z, x, y, params):
        try:
            x, y, z = int(x), int(y), int(z)
        except ValueError:
            raise RestException('x, y, and z must be integers', code=400)
        if x < 0 or y < 0 or z < 0:
            raise RestException('x, y, and z must be positive integers',
                                code=400)

        tileSource = self._loadTileSource(id, params)
        try:
            tileData = tileSource.getTile(x, y, z)
        except TileSourceException as e:
            raise RestException(e.message, code=404)

        cherrypy.response.headers['Content-Type'] = tileSource.getTileMimeType()
        return lambda: tileData
    """

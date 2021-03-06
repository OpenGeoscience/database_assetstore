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

import cherrypy
import json
import re
import six
from bson.objectid import ObjectId
from six.moves import urllib

from girder.constants import AssetstoreType
from girder.exceptions import GirderException, ValidationException
from girder.models.assetstore import Assetstore
from girder.models.file import File
from girder.models.folder import Folder
from girder.models.item import Item
from girder.models.setting import Setting
from girder.utility.abstract_assetstore_adapter import AbstractAssetstoreAdapter, FileHandle
from girder.utility import assetstore_utilities

from . import dbs
from .base import PluginSettings, DB_ASSETSTORE_USER_TYPE, DB_INFO_KEY
from .query import dbFormatList, queryDatabase, preferredFormat


class DatabaseAssetstoreFile(dict):
    """
    This wraps a Girder file object dictionary so that the size parameter can
    be determined only when actually necessary.
    """
    def __init__(self, file, adapter, *args, **kwargs):
        """
        Create a file dictionary where 'size' and 'mimeType' are determined
        lazily.

        :param file: the original file dictionary.
        :param adapter: the owning assetstore adapter.
        """
        self._file = file
        self._adapter = adapter
        self._loaded = None
        return super(DatabaseAssetstoreFile, self).__init__(file, *args, **kwargs)

    def __getitem__(self, key, *args, **kwargs):
        """
        If this is the first time size is asked for, compute it and store it in
        the original file dictionary.  Otherwise, just return the internal
        dictionary result.

        See the base dict class for function details.
        """
        if key in ('size', 'mimeType') and not self._loaded:
            self._adapter.downloadFile(self._file, offset=1, headers=False, endByte=2)
            dbinfo = getDbInfoForFile(self._file, self._adapter.assetstore)
            params = getQueryParamsForFile(self._file, True)
            resultFunc, mimeType = queryDatabase(self._file.get('_id'), dbinfo, params)
            self._file['mimeType'] = mimeType
            resultFunc, endByte = self._adapter._getDownloadSize(self._file, resultFunc, 0, 0)
            self._loaded = True
        return super(DatabaseAssetstoreFile, self).__getitem__(key, *args, **kwargs)


class DatabaseAssetstoreAdapter(AbstractAssetstoreAdapter):
    def __init__(self, assetstore):
        super(DatabaseAssetstoreAdapter, self).__init__(assetstore)

    @staticmethod
    def validateInfo(doc):
        """
        Ensures we have the necessary information to connect to a database.
        """
        # Ensure that the assetstore is marked read-only
        doc['readOnly'] = True
        info = doc.get('database', {})
        dbtype = info.get('dbtype')
        if dbtype == DB_ASSETSTORE_USER_TYPE:
            return
        uri = info.get('uri')
        if not uri:
            raise ValidationException('Missing uri field.')
        validatedDialect, validatedDbtype = dbs.getDBConnectorClassFromDialect(uri)
        if validatedDbtype is None:
            if dbtype:
                raise ValidationException(
                    'URI is not valid for dbtype %s.' % dbtype)
            raise ValidationException(
                'Either specify dbtype or include a dialect in the URI.')
        if '://' not in info['uri'] and validatedDialect:
            info['uri'] = '%s://%s' % (validatedDialect, info['uri'])
        info['dbtype'] = validatedDbtype
        connClass = dbs.getDBConnectorClass(info['uri'])
        info['uri'] = connClass.canonicalDatabaseUri(info['uri'])
        if connClass.databaseNameRequired and not dbs.databaseFromUri(info['uri']):
            raise ValidationException(
                'The specified database uri must include the database name.')

    def initUpload(self, upload):
        raise NotImplementedError('Database assetstores are read only.')

    def finalizeUpload(self, upload, file):
        raise NotImplementedError('Database assetstores are read only.')

    def deleteFile(self, file):
        if file.get(DB_INFO_KEY, {}).get('imported'):
            return
        raise NotImplementedError('Database assetstores are read only.')

    def setContentHeaders(self, file, offset, endByte, contentDisposition=None):
        """
        Sets the Content-Length, Content-Disposition, Content-Type, and also
        the Content-Range header if this is a partial download.

        :param file: The file being downloaded.
        :param offset: The start byte of the download.
        :type offset: int
        :param endByte: The end byte of the download (non-inclusive).
        :type endByte: int
        :param contentDisposition: Content-Disposition response header
            disposition-type value, if None, Content-Disposition will
            be set to 'attachment; filename=$filename'.
        :type contentDisposition: str or None
        """
        cherrypy.response.headers['Content-Type'] = \
            file.get('mimeType') or 'application/octet-stream'
        if contentDisposition == 'inline':
            cherrypy.response.headers['Content-Disposition'] = \
                'inline; filename="%s"' % file['name']
        else:
            cherrypy.response.headers['Content-Disposition'] = \
                'attachment; filename="%s"' % file['name']
        cherrypy.response.headers['Accept-Ranges'] = 'bytes'
        if (endByte is not None and offset is not None and
                file['size'] is not None):
            cherrypy.response.headers['Content-Length'] = max(
                endByte - offset, 0)
            if (offset or endByte < file['size']) and file['size']:
                cherrypy.response.headers['Content-Range'] = \
                    'bytes %d-%d/%d' % (offset, endByte - 1, file['size'])

    def _getDownloadSize(self, file, resultFunc, offset, endByte):
        """
        Given a file and an output generator function, generate the output to
        determine its total length, buffering the part that is needed for the
        final output.  Adjust the file's size and the endByte value
        accordingly.

        :param file: the file used for the original generation.  Its size value
                     is updated.
        :param resultFunc: a function that produces a generator for the output.
        :param offset: offset within the file information to output.
        :param endByte: the maximum index to output (the output is
                        data[offset:endByte]).
        :returns: a new function that produces a generator for the output.
        :returns: a new value for endByte.
        """
        totallen = 0
        skipped = 0
        output = six.BytesIO()
        for chunk in resultFunc():
            if not isinstance(chunk, six.binary_type):
                chunk = chunk.encode('utf8')
            totallen += len(chunk)
            if skipped < offset:
                if skipped + len(chunk) <= offset:
                    skipped += len(chunk)
                    continue
                else:
                    chunk = chunk[offset - skipped:]
                    skipped = offset
            if endByte and totallen > endByte:
                chunk = chunk[:endByte - totallen]
            output.write(chunk)
        output.seek(0)
        file['size'] = totallen

        def bufferResults():
            while True:
                chunk = output.read(65536)
                if not chunk:
                    break
                yield chunk

        if endByte is None or endByte > file['size']:
            endByte = file['size']
        return bufferResults, endByte

    def downloadFile(self, file, offset=0, headers=True, endByte=None,
                     contentDisposition=None, extraParameters=None, **kwargs):
        """
        This method is in charge of returning a value to the RESTful endpoint
        that can be used to download the file.  This can return a generator
        function that streams the file directly, or can modify the cherrypy
        request headers and perform a redirect and return None, for example.

        :param file: The file document being downloaded.
        :type file: dict
        :param offset: Offset in bytes to start the download at.
        :type offset: int
        :param headers: Flag for whether headers should be sent on the response.
        :type headers: bool
        :param endByte: Final byte to download. If ``None``, downloads to the
            end of the file.
        :type endByte: int or None
        :param contentDisposition: Value for Content-Disposition response
            header disposition-type value.
        :type contentDisposition: str or None
        :type extraParameters: str, dictionary, or None.  url encoded query
            string, JSON-encoded dictionary, or a Python dictionary of
            parameters to add to the query.
        :returns: a function that returns a generator for the data.
        """
        dbinfo = getDbInfoForFile(file, self.assetstore)
        params = getQueryParamsForFile(file, True)
        if extraParameters:
            if isinstance(extraParameters, six.string_types):
                try:
                    extraParameters = json.loads(extraParameters)
                except ValueError:
                    extraParameters = {
                        key: value for (key, value) in urllib.parse.parse_qsl(
                            extraParameters, keep_blank_values=True)}
            if not isinstance(extraParameters, dict):
                raise GirderException(
                    'The extraParameters field must either be a dictionary, a '
                    'JSON-encoded dictionary, or a url query-encoded string.')
            params.update(extraParameters)
            if params.get('limit', 'notpresent') is None:
                params['limit'] = 'none'
        resultFunc, mimeType = queryDatabase(file.get('_id'), dbinfo, params)
        # If we have been asked for inline data, change some mime types so
        # most browsers will show the data inline, even if the actual mime type
        # should be different (csv files are the clear example).
        if contentDisposition == 'inline' and mimeType not in (
                'application/json', 'text/plain'):
            mimeType = 'text/plain'
        file['mimeType'] = mimeType

        # If we have been asked for headers, recheck if we should have a range
        # request
        if headers and cherrypy.request.headers.get('Range'):
            rangeHeader = cherrypy.lib.httputil.get_ranges(
                cherrypy.request.headers.get('Range'), six.MAXSIZE)
            if rangeHeader and len(rangeHeader):
                # Currently we only support a single range.
                offset, endByte = rangeHeader[0]

        resultFunc = reyieldBytesFunc(resultFunc)

        # We often have to compute the response length.  This also handles
        # partial range requests (though not very efficiently, as each
        # request will requery the database, which may not be consistent).
        file['size'] = None
        if offset or endByte is not None:
            resultFunc, endByte = self._getDownloadSize(
                file, resultFunc, offset, endByte)

        if headers:
            self.setContentHeaders(file, offset, endByte, contentDisposition)
            if endByte is not None and endByte - offset <= 0:
                return lambda: b''

        return resultFunc

    def open(self, file):
        """
        Exposes a Girder file as a python file-like object. At the
        moment, this is a read-only interface, the equivalent of opening a
        system file with 'rb' mode.

        :param file: A Girder file document.
        :type file: dict
        :return: A file-like object containing the bytes of the file.
        :rtype: FileHandle
        """
        if not isinstance(file, DatabaseAssetstoreFile):
            file = DatabaseAssetstoreFile(file, self)
        return FileHandle(file, self)

    def copyFile(self, srcFile, destFile):
        """
        This method copies the necessary fields and data so that the
        destination file contains the same data as the source file.

        :param srcFile: The original File document.
        :type srcFile: dict
        :param destFile: The File which should have the data copied to it.
        :type destFile: dict
        :returns: A dict with the destination file.
        """
        return destFile

    def importData(self, parent, parentType, params, progress, user, **kwargs):
        """
        Import a list of tables, each to a file within a distinct item.  Each
        table specification in the list is an object which must have a 'table'
        key.  It may optionally have other connection information such as
        'database' and 'schema'.  If there is a 'name' key, the name is used
        for the item and file.  If there is a 'database' key, a subfolder is
        created within the specified parent with that name.  If a user or
        collection is specified for the top level and no database key is
        specified, the default database name (from the assetstore) is used.
        If the specific item and file already exists and is from the same
        assetstore, it is updated.  If the specific item already exists and is
        not from the same assetstore (or not marked that it was imported), an
        error is given.

        :param parent: The parent object to import into.  Must be a folder,
            user, collection, item, or file.
        :param parentType: The model type of the parent object.
        :param params: Additional parameters required for the import process:
            tables: a list of tables to add.  If there is already an item with
                    an exact table name, it is updated.
            sort: default sort parameter.  Used in plain downloads.
            fields: default fields parameter.  Used in plain downloads.
            filters: default filters parameter.  Used in plain downloads.
            group: default group parameter.  Used in plain downloads.
            format: default format parameter.  Used in plain downloads.
            replace: if False, don't replace an existing file/item with the
                name, but always create new entries.  A parentType of file
                will always replace the existing data of a file
        :type params: dict
        :param progress: Object on which to record progress if possible.
        :type progress: :py:class:`girder.utility.progress.ProgressContext`
        :param user: The Girder user performing the import.
        :type user: dict or None
        :return: a list of objects, each of which has an item and file entry
            with the items and files that were imported.
        """
        uri = (self.assetstore['database'].get('uri')
               if self.assetstore['database'].get('uri') else params['uri'])
        defaultDatabase = dbs.databaseFromUri(uri)
        response = []
        createdFolder = createdItem = createdFile = False
        for table in params['tables']:
            if isinstance(table, six.string_types):
                dbinfo = {'table': table}
            else:
                dbinfo = table.copy()
            if not self.assetstore['database'].get('uri'):
                dbinfo['uri'] = uri
            name = dbinfo.pop('name', dbinfo['table'])
            progress.update(message='Importing %s' % name)
            # Find or create a folder if needed
            if 'database' not in dbinfo and parentType == 'folder':
                folder = parent
            elif parentType not in ('file', 'item'):
                folderName = dbinfo.get('database', defaultDatabase)
                folder = Folder().findOne({
                    'parentId': parent['_id'],
                    'name': folderName,
                    'parentCollection': parentType
                })
                if folder is None:
                    folder = Folder().createFolder(
                        parent, folderName, parentType=parentType,
                        creator=user)
                    createdFolder = True
            if parentType == 'file':
                # for files, we'll create a provisional file below, then
                # delete the original assetstore entry and modify the
                # existing file entry with the updated values before saving.
                item = Item().load(parent['itemId'], force=True)
            elif parentType == 'item':
                item = parent
            else:
                # Create an item if needed
                item = Item().findOne({
                    'folderId': folder['_id'],
                    'name': name
                })
                if item is None or params.get('replace') is False:
                    item = Item().createItem(
                        name=name, creator=user, folder=folder)
                createdItem = True
            # Create a file if needed
            file = File().findOne({
                'name': name,
                'itemId': item['_id']
            })
            if file is None or params.get('replace') is False or parentType == 'file':
                file = File().createFile(
                    creator=user, item=item, name=name, size=0,
                    assetstore=self.assetstore,
                    mimeType=dbFormatList.get(preferredFormat(params.get(
                        'format'))),
                    saveFile=False)
                createdFile = True
            if file.get(DB_INFO_KEY) and not file[DB_INFO_KEY].get('imported'):
                raise GirderException(
                    'A file for table %s is present but cannot be updated '
                    'because it wasn\'t imported.' % name)
            try:
                file = self._importDataFile(file, parent, parentType, dbinfo, params)
            except GirderException as exc:
                self._importDataCleanup(
                    file if createdFile else None,
                    item if createdItem else None,
                    folder if createdFolder else None)
                raise exc
            response.append({'item': item, 'file': file})
        return response

    def _importDataFile(self, file, parent, parentType, dbinfo, params):
        """
        Validate and finish importing a file.

        :param file: The file to store information in.  If the parentType is
            file, the parent is updated instead.
        :param parent: The parent object to import into.
        :param parentType: The model type of the parent object.
        :param dbinfo: a dictionary of database information for the new file.
        :param params: Additional parameters required for the import process.
            See importData.
        :return: the file that was saved.
        """
        # Validate the limit parameter
        try:
            if params.get('limit') not in (None, ''):
                params['limit'] = int(params['limit'])
        except ValueError:
            raise GirderException(
                'limit must be empty or an integer')
        # Set or replace the database parameters for the file
        dbinfo['imported'] = True
        for key in ('sort', 'fields', 'filters', 'group', 'format',
                    'limit'):
            dbinfo[key] = params.get(key)
        file[DB_INFO_KEY] = dbinfo
        # Validate that we can perform queries by trying to download 1 record
        # from the file.
        #   This intentionally encodes extraParameters as JSON.  It could pass
        # it as a python dictionary or encode it as a url query string, but
        # another Girder plugin has expressed a preference for JSON as the de
        # facto standard for extraParameters.
        downloadFunc = self.downloadFile(
            file.copy(), headers=False, extraParameters=json.dumps({
                'limit': 1}))
        # Test the download without keeping it
        for chunk in downloadFunc():
            pass
        if parentType == 'file':
            assetstore_utilities.getAssetstoreAdapter(
                Assetstore().load(parent['assetstoreId'])).deleteFile(parent)
            for key in ('creatorId', 'created', 'assetstoreId', 'size',
                        DB_INFO_KEY):
                parent[key] = file[key]
            file = parent
        # Now save the new file
        File().save(file)
        return file

    def _importDataCleanup(self, file=None, item=None, folder=None):
        """
        Remove the specified items when an import fails.
        """
        if file and file.get('_id'):
            File().remove(file)
        if item and item.get('_id'):
            Item().remove(item)
        if folder and folder.get('_id'):
            Folder().remove(folder)

    def getTableList(self, uri=None, internalTables=False):
        """
        Return the list of known tables or collections.

        :param internalTables: True to include database internal tables (such
            as information_schema tables).
        :param uri: the uri to use for a user-database.
        :returns: a list of known tables.
        """
        return getTableList(self.assetstore, uri=None, internalTables=internalTables)

    def getDBConnectorForTable(self, table=None, overrideDbinfo={}):
        """
        Get a database connector for a specific table or otherwise override the
        adapters database info.

        :param table: table name.  This is used as the table and schema for the
            dbinfo.  If there is no period in the table name, only the table
            is changed.
        :param overrideDbinfo: extra information that overrides the adapter's
            default information.
        """
        uri = self.assetstore['database']['uri']
        dbinfo = {
            'uri': uri,
        }
        schema = None
        if table and '.' in table:
            schema, table = table.split('.', 1)
        if table:
            dbinfo['table'] = dbinfo['collection'] = table
        if schema:
            dbinfo['schema'] = schema
        dbinfo.update(overrideDbinfo)
        connClass = dbs.getDBConnectorClass(dbinfo.get('uri'))
        conn = connClass(**dbinfo)
        return conn

    def queryDatabase(self, connector, params):
        """
        Given a connector to this adapter, query the database.

        :param connector: a connector that is derived from the
            DatabaseConnector class.
        :param params: query parameters.  See the select endpoint for
            documentation.
        :returns: a result function that returns a generator that yields the
            results, or None for failed.
        :returns: the mime type of the results, or None for failed.
        """
        return queryDatabase(connector, None, params)


def getDbInfoForFile(file, assetstore=None):
    """
    Given a file document, get the necessary information to connect to a
    database.

    :param file: the file document.
    :param assetstore: the assetstore document, or None to get it from the
                       file information.
    :return: the dbinfo dictionary or None if the file is not in a database
             assetstore.
    """
    if DB_INFO_KEY not in file or 'assetstoreId' not in file:
        return None
    if assetstore is None:
        assetstore = Assetstore().load(file['assetstoreId'])
    if assetstore.get('type') != AssetstoreType.DATABASE:
        return None
    if assetstore['database'].get('dbtype') == DB_ASSETSTORE_USER_TYPE:
        uri = file[DB_INFO_KEY]['uri']
    else:
        uri = assetstore['database']['uri']
    dbinfo = {
        'uri': uri,
        'table': file[DB_INFO_KEY]['table'],
        'collection': file[DB_INFO_KEY]['table']

    }
    for key in ('database', 'schema'):
        if key in file[DB_INFO_KEY]:
            dbinfo[key] = file[DB_INFO_KEY][key]
    return dbinfo


def getQueryParamsForFile(file, setBlanks=False):
    """
    Given a file document, get the default query parameters.

    :param file: the file document.
    :param setBlanks: if True, set values even if blank, and include a zero
                      offset.
    :return: the default query parameters.
    """
    params = {}
    if DB_INFO_KEY not in file:
        return params
    if setBlanks:
        params['offset'] = 0
    for key in ('sort', 'fields', 'filters', 'group', 'format', 'limit'):
        if key in file[DB_INFO_KEY] or setBlanks:
            params[key] = file[DB_INFO_KEY].get(key)
    if str(params.get('limit')).isdigit():
        params['limit'] = int(params['limit'])
    return params


def getTableList(assetstore, uri=None, internalTables=False):
    """
    Given an assetstore, return the list of known tables or collections.

    :param assetstore: the assetstore document.
    :param uri: the uri to use for a user-database.
    :param internalTables: True to include database internal tables (such as
        information_schema tables).
    :returns: a list of known tables.
    """
    uri = uri if uri else assetstore['database']['uri']
    cls = dbs.getDBConnectorClass(uri)
    if cls is None:
        return []
    return cls.getTableList(
        uri,
        internalTables=internalTables,
        dbparams=assetstore['database'].get('dbparams', {}))


def reyieldBytesFunc(func):
    """
    Given a generator function, return a generator function that always yields
    bytes, never unicode.
    """
    def resultFunc():
        for chunk in func():
            if not isinstance(chunk, six.binary_type):
                chunk = chunk.encode('utf8')
            yield chunk

    return resultFunc


def validateFile(file):
    """
    If a file document contains the DB_INFO_KEY, check if it is in a database
    assetstore.  If so, check that the data in DB_INFO_KEY is valid.  Note
    that this won't check files without the DB_INFO_KEY, even if they are in
    the database assetstore to allow files to be created and then have
    database information added to them.

    :param file: the file document.
    """
    if DB_INFO_KEY not in file or 'assetstoreId' not in file:
        return None
    assetstore = Assetstore().load(file['assetstoreId'])
    if assetstore.get('type') != AssetstoreType.DATABASE:
        # This can happen if the file was a database_assetstore file and then
        # is replaced, for instance, by uploading a new file.
        if DB_INFO_KEY in file:
            del file[DB_INFO_KEY]
        return None
    if not file[DB_INFO_KEY].get('table'):
        raise ValidationException(
            'File database information entry must have a non-blank table '
            'value.')
    if not assetstore['database'].get('uri') and not file[DB_INFO_KEY].get('uri'):
        raise ValidationException(
            'File database information must have a non-blank uri value on an '
            'assetstore that doesn\'t specify a single database.')


def checkUserImport(user, uri, validateUri=True):
    """
    Check if the specified user is allowed to import the specified URI.

    :param user: the user to validate.
    :param uri: the database connection string to validate.
    :param validateUri: if False, only report if the user could import any
        database (the uri parameter is always considered matching).
    :returns: None if the user may import the specified URI, otherwise an error
        string.
    """
    if not Setting().get(PluginSettings.USER_DATABASES):
        return 'User-level database imports are not allowed'
    groups = Setting().get(PluginSettings.USER_DATABASES_GROUPS)
    # Admins can always do this if the base setting allows it
    if user.get('admin'):
        return
    if groups:
        error = 'group'
        for groupRule in groups:
            if groupRule['groupId']:
                groupId = groupRule['groupId']
                if not isinstance(groupId, ObjectId):
                    groupId = ObjectId(groupId)
                if groupId not in user.get('groups', []):
                    # The user is not part of this group
                    continue
            if validateUri and (not uri or not re.search(groupRule['pattern'], uri)):
                error = 'uri'
            else:
                # The user is part of this group and the pattern matches
                error = None
                break
        if error == 'group':
            return 'This user cannot add a database.'
        if error:
            return 'This user cannot add a database with this URI.'
    # allow the import

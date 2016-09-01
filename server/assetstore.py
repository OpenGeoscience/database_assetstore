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
import six
from six.moves import urllib

from girder.constants import AssetstoreType
from girder.models.model_base import GirderException, ValidationException
from girder.utility.abstract_assetstore_adapter import AbstractAssetstoreAdapter
from girder.utility.model_importer import ModelImporter

from .dbs import getDBConnectorClassFromDialect, databaseFromUri, \
    getDBConnectorClass
from .query import dbFormatList, queryDatabase, preferredFormat


dbInfoKey = 'databaseMetadata'


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
        uri = info.get('uri')
        if not uri:
            raise ValidationException('Missing uri field.')
        dbtype = info.get('dbtype')
        dialect = uri.split('://', 1)[0] if '://' in uri else dbtype
        validatedDialect, validatedDbtype = getDBConnectorClassFromDialect(
            dialect, dbtype)
        if validatedDbtype is None:
            if dbtype:
                raise ValidationException(
                    'URI is not valid for dbtype %s.' % dbtype)
            raise ValidationException(
                'Either specify dbtype or include a dialect in the URI.')
        info['uri'] = uri.split('://', 1)[-1]
        if validatedDialect:
            info['uri'] = '%s://%s' % (validatedDialect, info['uri'])
        info['dbtype'] = validatedDbtype
        connClass = getDBConnectorClass(info['dbtype'])
        if connClass.databaseNameRequired and not databaseFromUri(info['uri']):
            raise ValidationException(
                'The specified database uri must include the database name.')

    def initUpload(self, upload):
        raise NotImplementedError('Database assetstores are read only.')

    def finalizeUpload(self, upload, file):
        raise NotImplementedError('Database assetstores are read only.')

    def deleteFile(self, file):
        if file.get(dbInfoKey, {}).get('imported'):
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
                return lambda: ''

        return resultFunc

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
            user, or collection.
        :param parentType: The model type of the parent object.
        :param params: Additional parameters required for the import process:
            tables: a list of tables to add.  If there is already an item with
                    an exact table name, it is updated.
            sort: default sort parameter.  Used in plain downloads.
            fields: default fields parameter.  Used in plain downloads.
            filters: default filters parameter.  Used in plain downloads.
            format: default format parameter.  Used in plain downloads.
        :type params: dict
        :param progress: Object on which to record progress if possible.
        :type progress: :py:class:`girder.utility.progress.ProgressContext`
        :param user: The Girder user performing the import.
        :type user: dict or None
        """
        defaultDatabase = databaseFromUri(self.assetstore['database']['uri'])
        itemModel = self.model('item')
        fileModel = self.model('file')
        folderModel = self.model('folder')
        for table in params['tables']:
            if isinstance(table, six.string_types):
                dbinfo = {'table': table}
            else:
                dbinfo = table.copy()
            name = dbinfo.pop('name', dbinfo['table'])
            progress.update(message='Importing %s' % name)
            # Find or create a folder if needed
            if 'database' not in dbinfo and parentType == 'folder':
                folder = parent
            else:
                folderName = dbinfo.get('database', defaultDatabase)
                folder = folderModel.findOne({
                    'parentId': parent['_id'],
                    'name': folderName,
                    'parentCollection': parentType
                })
                if folder is None:
                    folder = folderModel.createFolder(
                        parent, folderName, parentType=parentType,
                        creator=user)
            # Create an item if needed
            item = itemModel.findOne({
                'folderId': folder['_id'],
                'name': name
            })
            if item is None:
                item = itemModel.createItem(
                    name=name, creator=user, folder=folder)
            # Create a file if needed
            file = fileModel.findOne({
                'name': name,
                'itemId': item['_id']
            })
            if file is None:
                file = fileModel.createFile(
                    creator=user, item=item, name=name, size=0,
                    assetstore=self.assetstore,
                    mimeType=dbFormatList.get(preferredFormat(params.get(
                        'format'))),
                    saveFile=False)
            if file.get(dbInfoKey) and not file[dbInfoKey].get('imported'):
                raise GirderException(
                    'A file for table %s is present but cannot be updated '
                    'because it wasn\'t imported.' % name)
            # Validate the limit parameter
            try:
                if params.get('limit') not in (None, ''):
                    params['limit'] = int(params['limit'])
            except ValueError:
                raise GirderException(
                    'limit must be empty or an integer')
            # Set or replace the database parameters for the file
            dbinfo['imported'] = True
            for key in ('sort', 'fields', 'filters', 'format', 'limit'):
                dbinfo[key] = params.get(key)
            file[dbInfoKey] = dbinfo
            # Validate that we can perform queries by trying to download 1
            # record from the file.
            #   This intentionally encodes extraParameters as JSON.  It could
            # pass it as a python dictionary or encode it as a url query string,
            # but another Girder plugin has expressed a preference for JSON as
            # the de facto standard for extraParameters.
            downloadFunc = self.downloadFile(
                file.copy(), headers=False, extraParameters=json.dumps({
                    'limit': 1}))
            # Test the download without keeping it
            [None for chunk in downloadFunc()]
            # Now save the new file
            fileModel.save(file)


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
    if dbInfoKey not in file or 'assetstoreId' not in file:
        return None
    if assetstore is None:
        assetstore = ModelImporter.model('assetstore').load(
            file['assetstoreId'])
    if assetstore.get('type') != AssetstoreType.DATABASE:
        return None
    dbinfo = {
        'type': assetstore['database']['dbtype'],
        'url': assetstore['database']['uri'],
        'table': file[dbInfoKey]['table'],
        'collection': file[dbInfoKey]['table']

    }
    for key in ('database', 'schema'):
        if key in file[dbInfoKey]:
            dbinfo[key] = file[dbInfoKey][key]
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
    if dbInfoKey not in file:
        return params
    if setBlanks:
        params['offset'] = 0
    for key in ('sort', 'fields', 'filters', 'format', 'limit'):
        if key in file[dbInfoKey] or setBlanks:
            params[key] = file[dbInfoKey].get(key)
    if str(params.get('limit')).isdigit():
        params['limit'] = int(params['limit'])
    return params


def validateFile(file):
    """
    If a file document contains the dbInfoKey, check if it is in a database
    assetstore.  If so, check that the data in dbInfoKey is valid.  Note
    that this won't check files without the dbInfoKey, even if they are in
    the database assetstore to allow files to be created and then have
    database information added to them.

    :param file: the file document.
    """
    if dbInfoKey not in file or 'assetstoreId' not in file:
        return None
    assetstore = ModelImporter.model('assetstore').load(
        file['assetstoreId'])
    if assetstore.get('type') != AssetstoreType.DATABASE:
        return None
    if not file[dbInfoKey].get('table'):
        raise ValidationException(
            'File database information entry must have a non-blank table '
            'value.')
    if (not databaseFromUri(assetstore['database']['uri']) and
            not file[dbInfoKey].get('database')):
        raise ValidationException(
            'File database information must have a non-blank database value '
            'on an assetstore that doesn\'t specify a single database.')

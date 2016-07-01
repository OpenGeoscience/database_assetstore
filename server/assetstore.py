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
import six
from six.moves import urllib

from girder.models.model_base import GirderException, ValidationException
from girder.utility.abstract_assetstore_adapter import AbstractAssetstoreAdapter

from .dbs import getDBConnectorClassFromDialect, dbFormatList, queryDatabase


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

    def initUpload(self, upload):
        raise NotImplementedError('Database assetstores are read only.')

    def finializeUpload(self, upload, file):
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
        :type extraParameters: str or None
        """
        dbinfo = {
            'type': self.assetstore['database']['dbtype'],
            'url': self.assetstore['database']['uri'],
            'table': file[dbInfoKey]['table'],
            'collection': file[dbInfoKey]['table']
        }
        params = {
            'sort': file[dbInfoKey].get('sort'),
            'fields': file[dbInfoKey].get('fields'),
            'filters': file[dbInfoKey].get('filters'),
            'format': file[dbInfoKey].get('format'),
            'offset': 0,
            'limit': file[dbInfoKey].get('limit'),
        }
        if extraParameters:
            params.update({key: value for (key, value) in
                           urllib.parse.parse_qsl(extraParameters)})
        resultFunc, mimeType = queryDatabase(file['_id'], dbinfo, params)
        file['mimeType'] = mimeType

        # We often have to compute the response length.  This also handles
        # partial range requests (though not very efficiently, as each
        # request will requery the database, which may not be consistent).
        file['size'] = None
        if offset or endByte is not None:
            totallen = 0
            skipped = 0
            output = six.BytesIO()
            for chunk in resultFunc():
                totallen += len(chunk)
                if skipped < offset:
                    if skipped + len(chunk) <= offset:
                        skipped += len(chunk)
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

            resultFunc = bufferResults
            if endByte is None or endByte > file['size']:
                endByte = file['size']

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
        Import a list of tables or collections, each to a file within a
        distinct item.

        :param parent: The parent object to import into.  Must be a folder.
        :param parentType: The model type of the parent object.  Ignored, as it
            must be 'folder'.
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
        itemModel = self.model('item')
        fileModel = self.model('file')
        for table in params['tables']:
            progress.update(message='Importing %s' % table)
            # Create an item if needed
            item = itemModel.findOne({
                'folderId': parent['_id'],
                'name': table
            })
            if item is None:
                item = itemModel.createItem(
                    name=table, creator=user, folder=parent)
            # Create a file if needed
            file = fileModel.findOne({
                'name': table,
                'itemId': item['_id']
            })
            if file is None:
                file = fileModel.createFile(
                    creator=user, item=item, name=table, size=0,
                    assetstore=self.assetstore,
                    mimeType=dbFormatList.get(params.get('format')),
                    saveFile=False)
            if file.get(dbInfoKey) and not file[dbInfoKey].get('imported'):
                raise GirderException(
                    'A file for table %s is present but cannot be updated '
                    'because it wasn\'t imported.' % table)
            # Set or replace the database parameters for the file
            file[dbInfoKey] = {
                'imported': True,
                'table': table,
                'sort': params.get('sort'),
                'fields': params.get('fields'),
                'filters': params.get('filters'),
                'format': params.get('format'),
                'limit': params.get('limit'),
            }
            # Validate that we can download from this?  # ##DWM::
            fileModel.save(file)

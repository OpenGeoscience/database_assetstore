import bson
import collections
import csv
import datetime
import decimal
import json
import six

from six.moves import range

from girder.models.model_base import GirderException

from . import dbs


dbFormatList = collections.OrderedDict([
    ('list', 'application/json'),
    ('dict', 'application/json'),
    ('csv', 'text/csv'),
    ('json', 'application/json'),  # Same as the data component of dict
    ('jsonlines', 'text/plain'),
    # http,//www.iana.org/assignments/media-types/application/vnd.geo+json
    ('geojson', 'application/vnd.geo+json'),
    ('rawdict', ''),
    ('rawlist', ''),
])


class DatabaseQueryException(GirderException):
    pass


# Functions related to querying databases

def convertSelectDataToCsv(result, dumpFunc=json.dumps, *args, **kargs):
    """
    Return a function that produces a generator for outputting a CSV file.

    :param result: the initial select results.
    :param dumpFunc: fallback function for dumping objects and unknown data
                     types to JSON.
    :returns: a function that outputs a generator.
    """
    class Echo(object):
        def write(self, value):
            return value

    writer = csv.writer(Echo())
    # values that are of a type in allowedTypes and not in disallowedTypes
    # should be converted by the CSV writer.  All others are converted to JSON
    # first.  The integer_types include True and False.  We may need to add
    # more type from sqlalchemy.sql.sqltypes.
    allowedTypes = six.string_types + six.integer_types + (
        float, type(None), datetime.datetime, decimal.Decimal)
    disallowedTypes = (bson.binary.Binary, )
    data = convertSelectDataToList(result)['data']

    def resultFunc():
        columns = {result['columns'][col]: col for col in result['columns']}
        columnNames = [columns[i] for i in range(len(result['fields']))]
        yield writer.writerow(csv_safe_unicode(columnNames))
        for row in data:
            row = [value if isinstance(value, allowedTypes) and
                   not isinstance(value, disallowedTypes) else
                   dumpFunc(value, check_circular=False, separators=(',', ':'),
                            sort_keys=False, default=str) for value in row]

            yield writer.writerow(csv_safe_unicode(row))

    return resultFunc


def convertSelectDataToDict(result, *args, **kargs):
    """
    Convert data in list format to dictionary format.  The column names are
    used as the keys for each row.

    :param result: the initial select results.  This can be altered.
    :returns: the results with data converted from a list of lists to a list of
              dictionaries.
    """
    if result['format'] != 'dict':
        result['data'] = convertSelectDataToJson(result)
        result['format'] = 'dict'
    return result


def convertSelectDataToGeojson(result, dumpFunc=json.dumps, *args, **kargs):
    """
    Convert data in list format to a single GeoJSON geometry collection.  All
    columns in all rows are merged into a single object.  If the entry is
    obviously not a GeoJSON object, it is excluded.

    :param result: the initial select results.
    :param dumpFunc: function for dumping objects to JSON.
    :returns: a function that outputs a generator.
    """
    data = convertSelectDataToList(result)['data']

    def resultFunc():
        geometryHeader = '{"type":"GeometryCollection","geometries":[\n'
        featureHeader = '{"type":"FeatureCollection","features":[\n'
        first = True
        for row in data:
            for entry in row:
                if isinstance(entry, six.string_types):
                    value = entry.strip()
                    if not value.startswith('{') or not value.endswith('}'):
                        value = None
                elif isinstance(entry, dict):
                    value = dumpFunc(
                        entry, check_circular=False, separators=(',', ':'),
                        sort_keys=False, default=str, indent=None).strip()
                else:
                    value = None
                if value:
                    if first:
                        coll = 'geo'
                        try:
                            jsondata = json.loads(value)
                            if jsondata['type'] == 'Feature':
                                coll = 'feature'
                        except Exception:
                            pass
                        if coll == 'geo':
                            yield geometryHeader
                        else:
                            yield featureHeader
                        yield value
                        first = False
                    else:
                        yield ',\n' + value
        if first:
            yield geometryHeader
        yield '\n]}'

    return resultFunc


def convertSelectDataToJson(result, *args, **kargs):
    """
    Convert data in list format to a simple JSON array format.  The column
    names are used as the keys for each row.

    :param result: the initial select results.
    :returns: the results with only the data.  The data is converted from a
              list of lists to a list of dictionaries.
    """
    data = result['data']
    if result['format'] == 'list':
        columns = {result['columns'][col]: col for col in result['columns']}
        data = [{columns[i]: row[i] for i in range(len(row))}
                for row in data]
    return data


def convertSelectDataToJsonlines(result, dumpFunc=json.dumps, *args, **kargs):
    """
    Convert data in list format to the Mongo JSON lines format.  This has each
    line be one data item represented as an independent JSON document.  The
    column names are used as the keys for each row.

    :param result: the initial select results.
    :param dumpFunc: function for dumping objects to JSON.
    :returns: a function that outputs a generator.
    """
    data = convertSelectDataToJson(result)

    def resultFunc():
        for row in data:
            yield dumpFunc(
                row, check_circular=False, separators=(',', ':'),
                sort_keys=False, default=str, indent=None).strip() + '\n'

    return resultFunc


def convertSelectDataToList(result, *args, **kargs):
    """
    Convert data in list format to dictionary format.  The column names are
    used as the keys for each row.

    :param result: the initial select results.  This can be altered.
    :returns: the results with data converted from a list of lists to a list of
              dictionaries.
    """
    if result['format'] == 'dict':
        columns = [col[-1] for col in sorted(
            [(result['columns'][col], col) for col in result['columns']])]
        result['data'] = [[row.get(col) for col in columns]
                          for row in result['data']]
        result['format'] = 'list'
    return result


def convertSelectDataToRawdict(result, *args, **kargs):
    """
    Convert data in list format to dictionary format.  The column names are
    used as the keys for each row.

    :param result: the initial select results.  This can be altered.
    :returns: a function that outputs aa generator.
    """
    if result['format'] != 'dict':
        result['data'] = convertSelectDataToJson(result)
        result['format'] = 'dict'

    def resultFunc():
        for row in result['data']:
            yield row

    return resultFunc


def convertSelectDataToRawlist(result, *args, **kargs):
    """
    Convert data in list format to dictionary format.  The column names are
    used as the keys for each row.

    :param result: the initial select results.  This can be altered.
    :returns: a function that outputs aa generator.
    """
    result = convertSelectDataToList(result)

    def resultFunc():
        for row in result['data']:
            yield row

    return resultFunc


def csv_safe_unicode(row, ignoreErrors=True):
    """
    Given an array of values, make sure all strings are unicode in Python 3 and
    str in Python 2.  The csv.writer in Python 2 does not handle unicode
    strings and in Python 3 it does not handle byte strings.

    :param row: an array which could contain mixed types.
    :param ignoreErrors: if True, convert what is possible and ignore errors.
        If false, conversion errors will raise an exception.
    :returns: either the original array if safe, or an array with all byte
        strings converted to unicode in Python 3 or str in Python 2.
    """
    # This is unicode in Python 2 and bytes in Python 3
    wrong_type = six.text_type if str == six.binary_type else six.binary_type
    # If all of the data is not the wrong type, just return it as is.  This
    # allows non-string types, such as integers and floats.
    if not any(isinstance(value, wrong_type) for value in row):
        return row
    # Convert the wrong type of string to the type needed for this version of
    # Python.  For Python 2 unicode gets encoded to str (bytes).  For Python 3
    # bytes get decoded to str (unicode).
    func = 'encode' if str == six.binary_type else 'decode'
    row = [getattr(value, func)('utf8', 'ignore' if ignoreErrors else 'strict')
           if isinstance(value, wrong_type) else value for value in row]
    return row


def getFilters(conn, fields, filtersValue=None, queryParams={},
               reservedParameters=[]):
    """
    Get a set of filters from a JSON list and/or from a set of query
    parameters.  Only query parameters of the form (field)[_(operator)] where
    the entire name is not in the reserver parameter list are processed.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  This is conn.getFieldInfo().
    :filtersValue: a JSON object with the desired filters or None or empty
                   string.
    :queryParameters: a dictionary of query parameters that can add additional
                      filters.
    :reservedParameters: a list or set of reserver parameter names.
    """
    filters = []
    if filtersValue not in (None, ''):
        if isinstance(filtersValue, six.string_types):
            try:
                filtersList = json.loads(filtersValue)
            except ValueError:
                filtersList = None
        else:
            filtersList = filtersValue
        filtersList = validateFilterList(filtersList)
        if not isinstance(filtersList, list):
            raise DatabaseQueryException(
                'The filters parameter must be a JSON list.')
        for filter in filtersList:
            filters.append(validateFilter(conn, fields, filter))
    if queryParams:
        for fieldEntry in fields:
            field = fieldEntry['name']
            for operator in dbs.FilterOperators:
                param = field + ('' if operator is None else '_' + operator)
                if param in queryParams and param not in reservedParameters:
                    filters.append(validateFilter(conn, fields, {
                        'field': field,
                        'operator': operator,
                        'value': queryParams[param]
                    }))
    return [filter for filter in filters if filter is not None]


def getFieldsList(conn, fields=None, fieldsValue=None, paramName='fields'):
    """
    Get a list of fields from the query parameters.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  None to let the connector fetch
                   them.
    :param fieldsValue: either a comma-separated list, a JSON list, or None.
    :param paramName: parameter name used in errors.
    :returns: a list of fields or None.
    """
    if fieldsValue is None or fieldsValue == '':
        return None
    if isinstance(fieldsValue, six.string_types) and '[' not in fieldsValue:
        fieldsList = [field.strip() for field in fieldsValue.split(',')
                      if len(field.strip())]
    else:
        if isinstance(fieldsValue, six.string_types):
            try:
                fieldsList = json.loads(fieldsValue)
            except ValueError:
                fieldsList = None
        else:
            fieldsList = fieldsValue
        if not isinstance(fieldsList, list):
            raise DatabaseQueryException(
                'The %s parameter must be a JSON list or a comma-separated '
                'list of known field names.' % paramName)
    for field in fieldsList:
        if not conn.isField(
                field, fields,
                allowFunc=getattr(conn, 'allowFieldFunctions', False)):
            raise DatabaseQueryException('%s must use known fields (%r unknown).' % (
                paramName.capitalize(), field))
    if not len(fieldsList):
        return None
    return fieldsList


def getSortList(conn, fields=None, sortValue=None, sortDir=None):
    """
    Get a list of sort fields and directions from the query parameters.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.  None to let the connector fetch
                   them.
    :param sortValue: either a sort field, a JSON list, or None.
    :param sortDir: if sortValue is a sort field, the sort direction.
    :returns: a list of sort parameters or None.
    """
    if sortValue is None or sortValue == '':
        return None
    sort = None
    if isinstance(sortValue, six.string_types) and '[' not in sortValue:
        if conn.isField(sortValue, fields) is not False:
            sort = [(
                sortValue,
                -1 if sortDir in (-1, '-1', 'desc', 'DESC') else 1
            )]
    else:
        if isinstance(sortValue, six.string_types):
            try:
                sortList = json.loads(sortValue)
            except ValueError:
                sortList = None
        else:
            sortList = sortValue
        if not isinstance(sortList, list):
            raise DatabaseQueryException(
                'The sort parameter must be a JSON list or a known field '
                'name.')
        sort = []
        for entry in sortList:
            if (isinstance(entry, list) and 1 <= len(entry) <= 2 and
                    conn.isField(
                        entry[0], fields,
                        allowFunc=getattr(conn, 'allowSortFunctions', False))
                    is not False):
                sort.append((
                    entry[0],
                    -1 if len(entry) > 1 and entry[1] in
                    (-1, '-1', 'desc', 'DESC') else 1
                ))
            elif (conn.isField(
                    entry, fields,
                    allowFunc=getattr(conn, 'allowSortFunctions', False))
                    is not False):
                sort.append((entry, 1))
            else:
                sort = None
                break
    if sort is None:
        raise DatabaseQueryException('Sort must use known fields.')
    return sort


def preferredFormat(format):
    """
    Given a format value, return the canonical format value or None if it is
    not a known format.

    :param format: the proposed format.
    :returns: the canonical format name or None if unknown.
    """
    format = str(format or 'list').replace('_', '').lower()
    if format not in dbFormatList:
        return None
    return format


def queryDatabase(idOrConnector, dbinfo, params):
    """
    Query a database.

    :param idOrConnector: either an id used to cache the DB connector, or a
        connector that is derived from the DatabaseConnector class.
    :param dbinfo: a dictionary of connection information for the db.  Needs
        type, uri, and either table or connection.  Ignored if a connector is
        provided.
    :param params: query parameters.  See the select endpoint for
        documentation.
    :returns: a result function that returns a generator that yields the
        results, or None for failed.
    :returns: the mime type of the results, or None for failed.
    """
    if isinstance(idOrConnector, dbs.DatabaseConnector):
        conn = idOrConnector
    else:
        conn = dbs.getDBConnector(idOrConnector, dbinfo)
    if not conn:
        raise dbs.DatabaseConnectorException('Failed to connect to database.')
    fields = conn.getFieldInfo()
    queryProps = {
        'limit':
            (-1) if params.get('limit') in ('none', 'None') else int(
                50 if params.get('limit') is None else params.get('limit')),
        'offset': int(params.get('offset', 0) or 0),
        'sort': getSortList(conn, fields, params.get('sort'),
                            params.get('sortdir')),
        'fields': getFieldsList(conn, fields, params.get('fields')),
        'wait': float(params.get('wait', 0)),
        'poll': float(params.get('poll', 10)),
        'initwait': float(params.get('initwait', 0)),
    }
    if 'group' in params:
        queryProps['group'] = getFieldsList(conn, fields, params['group'], 'group')
    client = params.get('clientid')
    format = preferredFormat(params.get('format'))
    if not format:
        raise DatabaseQueryException('Unknown output format.')
    filters = getFilters(conn, fields, params.get('filters'), params, {
        'limit', 'offset', 'sort', 'sortdir', 'fields', 'wait', 'poll',
        'initwait', 'clientid', 'filters', 'format', 'pretty'})
    result = conn.performSelectWithPolling(fields, queryProps, filters,
                                           client)
    if result is None:
        return None, None
    if 'fields' in result:
        result['columns'] = {
            result['fields'][col] if not isinstance(
                result['fields'][col], dict) else
            result['fields'][col].get('reference', 'column_' + str(col)):
            col for col in range(len(result['fields']))}
    if 'datacount' not in result:
        result['datacount'] = len(result.get('data', []))
    if not result.get('format'):
        result['format'] = 'list'  # This is the current format
    if result.get('format') not in ('list', 'dict'):
        raise DatabaseQueryException('Unknown internal format.')
    mimeType = dbFormatList.get(format, 'application/json')

    pretty = params.get('pretty') == 'true'
    dumpFunc = getattr(conn, 'jsonDumps', json.dumps)

    convertFunc = globals().get('convertSelectDataTo%s' % format.capitalize())
    if convertFunc:
        result = convertFunc(result, dumpFunc=dumpFunc, pretty=pretty)
    if callable(result):
        resultFunc = result
    else:
        # We could let Girder convert the results into JSON, but it is
        # marginally faster to dump the JSON ourselves, since we can exclude
        # sorting and reduce whitespace.

        def resultFunc():
            yield dumpFunc(
                result, check_circular=False, separators=(',', ':'),
                sort_keys=False, default=str, indent=2 if pretty else None)

    return resultFunc, mimeType


def validateFilter(conn, fields, filter):
    """
    Validate a filter by ensuring that the field exists, the operator is valid
    for that field's data type, and that any additional properties are allowed.
    Convert the filter into a fully populated dictionary style (one that has at
    least field, operator, and value).  Filters may also be a dictionary with
    group (either 'and' or 'or') and value (a list of filters).

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.
    :param filter: either a dictionary or a list or tuple with two to three
                   components representing (field), [(operator),] (value).
    :returns: the filter in dictionary-style or None if the filter has no
              effect.
    """
    if isinstance(filter, (list, tuple)):
        if len(filter) < 2 or len(filter) > 3:
            raise DatabaseQueryException(
                'Filters in list format must have two or three components.')
        if len(filter) == 2:
            filter = {'field': filter[0], 'value': filter[1]}
        else:
            filter = {
                'field': filter[0], 'operator': filter[1], 'value': filter[2]
            }
    if (filter.get('and') is not None or filter.get('or') is not None or
            filter.get('group')):
        return validateFilterGroup(conn, fields, filter)
    if filter.get('operator') not in dbs.FilterOperators:
        raise DatabaseQueryException('Unknown filter operator %r' % filter.get(
            'operator'))
    filter['operator'] = dbs.FilterOperators[filter.get('operator')]
    if 'field' not in filter and 'lvalue' in filter:
        filter['field'] = {'value': filter['lvalue']}
    if 'field' not in filter and ('func' in filter or 'lfunc' in filter):
        filter['field'] = {
            'func': filter.get('func', filter.get('lfunc')),
            'param': filter.get('param', filter.get('params', filter.get(
                'lparam', filter.get('lparams'))))
        }
    if 'value' not in filter and 'rfunc' in filter:
        filter['value'] = {
            'func': filter['rfunc'],
            'param': filter.get('rparam', filter.get('rparams'))
        }
    if 'field' not in filter:
        raise DatabaseQueryException('Filter must specify a field or func.')
    if (not conn.isField(
            filter['field'], fields,
            allowFunc=getattr(conn, 'allowFilterFunctions', False)) and
            not isinstance(filter['field'], dict) and
            'value' not in filter['field'] and 'func' not in filter['field']):
        raise DatabaseQueryException('Filters must be on known fields.')
    if 'value' not in filter:
        raise DatabaseQueryException('Filters must have a value or rfunc.')
    if not conn.checkOperatorDatatype(filter['field'], filter['operator'],
                                      fields):
        raise DatabaseQueryException('Cannot use %s operator on field %s' % (
            filter['operator'], filter['field']))
    return filter


def validateFilterGroup(conn, fields, filter):
    """
    Validate a filter group.  The filter must be a dictionary with either 'and'
    or 'or' with a list of filters, or 'group' with 'and' or 'or' and 'value'
    with a list of filters.  The result is either a filter, a filter group in
    the group/value form, or None.

    :param conn: the database connector.  Used for validating fields.
    :param fields: a list of known fields.
    :param filter: a dictionary with a filter group.
    :returns: a filter, filter group, or None.
    """
    if filter.get('and') is not None:
        group = 'and'
        value = filter['and']
        bad = filter.get('or') is not None or filter.get('group')
    elif filter.get('or') is not None:
        group = 'or'
        value = filter['or']
        bad = filter.get('group')
    else:
        group = filter['group']
        value = filter.get('value')
        bad = group not in ('and', 'or')
    if bad or not isinstance(value, (list)):
        raise DatabaseQueryException('Filter group badly formed.')
    value = [validateFilter(conn, fields, entry)
             for entry in validateFilterList(value)]
    value = [entry for entry in value if entry is not None]
    if not len(value):
        return None
    if len(value) == 1:
        return value[0]
    return {'group': group, 'value': value}


def validateFilterList(filters):
    """
    Check if an object is a list or tuple of filters.  If it is a single
    filter, convert it to a list.

    :param filters: a list of filters, a single filter, or None.
    :return filters: a list of filters or None if this cannot be a converted to
        a list of filters.
    """
    if filters is None:
        return
    if isinstance(filters, tuple):
        filters = list(filters)
    if (not isinstance(filters, list) or (
            len(filters) and isinstance(filters[0], six.string_types))):
        filters = [filters]
    return filters

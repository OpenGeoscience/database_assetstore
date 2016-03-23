Database Items |build-status| |license-badge|
=============================================
A Girder_ Plugin
----------------

A Girder plugin to provide access to database tables via extra item endpoints.

The ``POST`` ``item/{id}/database`` endpoint gets sent a JSON object that contains connection information, such as ``{"type": "sqlalchemy_postgres", "table": "(name of table)", "url": "postgresql://(postgres user):(postgres password)@(host name or IP address)/(name of database)"}``.  This could also more sophisticated connection information (for instance, a "dbparams" value with a dictionary of parameters for the connection, such as "connect_timeout").

At the moment, the type must be "postgres", but this is set up to be extended to other database types.

The ``GET`` ``item/{id}/database`` endpoint reports the values set with POST.

The ``DELETE`` ``item/{id}/database`` endpoint clears the values set with POST.

The ``GET`` ``item/{id}/database/fields`` endpoint reports a list of known field and their datatypes.

The ``GET`` ``item/{id}/database/select`` endpoint performs queries and returns data.  See its documentation for more information.  If the data in the database is actively changing, polling can be used to wait for data to appear.

The ``PUT`` ``item/{id}/database/refresh`` endpoint should be used if the available fields (columns) or functions of a database have changed.

Select Options
==============

The ``GET`` ``item/{id}/database/select`` endpoint has numerous options:

* *limit* - how many results to return.  0 for none (this stills performs the select).  Default is 50.
* *offset* - the offset to the first result to return.  Not really useful unless a sort is applied.
* *sort* - either a single field (column) name, in which case the *sortdir* option is used, **or** a comma-separated list of field names, **or** a JSON list of sort parameters.  When using a JSON list, each entry in the list can either be a column name, or can be a list with the first value the column name (or a function), and the second value the sort direction.

  For instance, ``type,town``, ``["type","town"]``, ``[["type",1],["town",1]]`` all sort the output first by type and then by town, both ascending.

  An example of a sort using a function: ``["type", [{"func": "lower", "param": {"field": "town"}}, -1]]`` will sort first by ascending type then by descending lower-case town.

* *sortdir* - this is only used if a single sort field is given in *sort*.  A positive number will sort in ascending order; a negative number in descending order.

* *fields* - the list of field (columns) to return.  By default, all known fields are returned in an arbitrary order.  This ensures a particular order and will only return the specified fields.  This may be either a comma-separated list of field names **or** a JSON list with either field names or functions.  If a function is specified, it can be given a ``reference`` key that will be used as a column name.

  An example of fetching fields including a function: ``["town", {"func": "lower", "param": {"field": "town"}, "reference": "lowertown"}]``

* *filters* - a JSON list of filters to apply.  Each filter is a list or an object.  If a list, the filter is of the form [(field or object with function or value), (operator), (value or object with field, function, or value)].  If a filter is specified with an object, it needs either "field", "func" and "param", or "lvalue" for the left side, "operator", and either "value" or "rfunc" and "rparam" for the right side.  The operator is optional, and if not specified is the equality test.  The "field" and "value" entries can be objects with "field", "value" or "func" and "param".

  Operators are dependant of field datatypes and the database connector that is used.  The following operators are available:
  * eq (=)
  * ne (!=, <>)
  * gte (>=, min)
  * gt (>)
  * lt (<, max)
  * lte (<=)
  * in - can have a list of values on the right side
  * not_in (notin) - can have a list of values on the right side
  * regex (~)
  * not_regex (notregex, !~)
  * search (~*) - generally a case-insensitive regex.  Some connectors could implement a stemming search instead
  * not_search (notsearch, !~*)
  * is 
  * notis (not_is, isnot, is_not)

  Example filters:
  
  * ``[["town", "BOSTON"]]`` - the town field must equal "BOSTON".
  * ``[{"field": "town", "operator": "eq", "value": "BOSTON"}]`` - the same filter as the previous one, just constructed differently.
  * ``[{"lvalue": "BOSTON", "value": {"field": "town"}}]`` - yet another way to construct the same filter.
  * ``[{"func": "lower", "param": [{"field": "town"}], "value": "boston"}]`` - the lower-case version of the town field must equal "boston".
  * ``[["pop2010", ">", 100000], {"field": "town", "operator": "ne", "value": "BOSTON"}]`` - the population in 2010 must be greater than 100,000, but don't mention Boston.

* *format* - data can be returned as a list of list, where each entry is a list of the returned fields, or as a list of dictionaries, where each entry is a map of the field names and the values.  The ``list`` format is more efficient.  The results are identical except for the data representation.

* *clientid* - an optional client ID can be specified with each request.  If this is included, and there is a pending select request from the same client ID, the pending request will be cancelled if possible.  This can be used when a client no longer needs the data from a first request because the new request will replace it.

* *wait* - if the data source is being actively changed, select can poll it periodically until there is data available.  If specified, this is a duration in seconds to poll the data.  As soon as data is found, it is returned.  If no data is found, the results are the same as not using wait.

* *poll* - if *wait* is used, this is the interval in seconds to check if data has changed based on the other select parameters.  Making this value too small will produce a high load on the database server.

* *initwait* - if *wait* is used, don't check for data for this duration in seconds, then start polling.  This can be used to reduce server load.

Database Functions
------------------

The ``sort``, ``fields``, and ``filters`` select parameters can use database functions.  Only non-internal, non-volatile functions are permitted.  For instance, when using Postgresql, you cannot use ``pg_*`` functions, nor a function like ``nextval``.

Functions can be nested -- a function can be used as the parameter of another function.

When using a Postgres database, many Postgres operations are exposed as functions.  For instance, using `float8mul` allows double-precision multiplication.

If a function takes a single parameter, the ``param`` value can be a single item.  Otherwise, it is a list of the values for the function.

Anywhere a function can be used (which includes the parameters of another function), a field (column) or a specified value can be used instead: ``{"field": (name of field)`` or ``{"value": (value)}``.

Here is example of a filter with a nested function (using PostGIS functions):

``[{"func": "st_intersects", "param": [{"func": "st_setsrid", "param": [{"func": "st_makepoint", "param": [-72, 42.36]}, 4326]}, {"func": "st_transform", "param": [{"field": "geom"}, 4326]}], "operator": "is", "value": true}]``


.. _Girder: https://github.com/girder/girder

.. |build-status| image:: https://travis-ci.org/OpenGeoscience/girder_db_items.svg?branch=master
    :target: https://travis-ci.org/OpenGeoscience/girder_db_items
    :alt: Build Status

.. |license-badge| image:: https://raw.githubusercontent.com/girder/girder/master/docs/license.png
    :target: https://pypi.python.org/pypi/girder
    :alt: License


Database Items |build-status| |license-badge|
=============================================
A Girder_ Plugin
----------------

A Girder plugin to provide access to database tables via extra item endpoints.

The `POST` `item/{id}/database` endpoint gets sent a JSON object that contains connection information, such as `{"type": "sqlalchemy_postgres", "table": "(name of table)", "url": "postgresql://(postgres user):(postgres password)@(host name or IP address)/(name of database)"}`.  This could also more sophisticated connection information (for instance, a "dbparams" value with a dictionary of parameters for the connection, such as "connect_timeout").

At the moment, the type must be "postgres", but this is set up to be extended to other database types.

The `GET` `item/{id}/database` endpoint reports the values set with POST.  The equivalent `DELETE` endpoint clears these values.

The `GET` `item/{id}/database/fields` endpoint reports a list of known field and their datatypes.

The `GET` `item/{id}/database/select endpoint` performs queries and returns data.  See its documentation for more information.  If the data in the database is actively changing, polling can be used to wait for data to appear.

.. _Girder: https://github.com/girder/girder

.. |build-status| image:: https://travis-ci.org/OpenGeoscience/girder_db_items.svg?branch=master
    :target: https://travis-ci.org/OpenGeoscience/girder_db_items
    :alt: Build Status

.. |license-badge| image:: https://raw.githubusercontent.com/girder/girder/master/docs/license.png
    :target: https://pypi.python.org/pypi/girder
    :alt: License


# We can't use the standard server tests since we have data files
add_standard_plugin_tests(NO_SERVER_TESTS)

add_python_style_test(python_static_analysis_${_pluginName}_tests "${_pluginDir}/plugin_tests")

add_python_test(assetstore PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/testdb.sql.gz")
add_python_test(dbs_mongo PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/mongodb.permits.json.bz2")
add_python_test(file PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/testdb.sql.gz")

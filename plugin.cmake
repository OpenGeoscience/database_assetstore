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

add_python_style_test(
  python_static_analysis_database_assetstore
  "${CMAKE_CURRENT_LIST_DIR}/server"
)

add_eslint_test(
  js_static_analysis_database_assetstore_gruntfile
  "${CMAKE_CURRENT_LIST_DIR}/Gruntfile.js"
)
add_eslint_test(
  js_static_analysis_database_assetstore_source
  "${CMAKE_CURRENT_LIST_DIR}/web_client"
)

add_python_test(assetstore PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/testdb.sql.gz")
add_python_test(dbs_mongo PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/mongodb.permits.json.bz2")
add_python_test(file PLUGIN database_assetstore BIND_SERVER EXTERNAL_DATA "plugins/database_assetstore/testdb.sql.gz")

add_web_client_test(example "${CMAKE_CURRENT_LIST_DIR}/plugin_tests/exampleSpec.js" PLUGIN database_assetstore)

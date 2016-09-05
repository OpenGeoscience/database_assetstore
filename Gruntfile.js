/**
 * Copyright 2016 Kitware Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module.exports = function (grunt) {
    var path = require('path');

    // This gruntfile is only designed to be used with girder's build system.
    // Fail if grunt is executed here.
    if (path.resolve(__dirname) === path.resolve(process.cwd())) {
        grunt.fail.fatal('To build database_assetstore, run grunt from Girder\'s root directory');
    }

    grunt.config.merge({
        plugin: {
            database_assetstore: {
                root: '<%= pluginDir %>/database_assetstore',
                static: '<%= staticDir %>/built/plugins/database_assetstore'
                // node_modules: '<%= plugin.database_assetstore.root %>/node_modules'
            }
        }
    });
};

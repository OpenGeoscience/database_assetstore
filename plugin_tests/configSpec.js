girderTest.importPlugin('database_assetstore');

girderTest.startApp();

describe('Test plugin configuration page', function () {
    it('change the settings', function () {
        girderTest.login('admin', 'Admin', 'Admin', 'adminpassword')();
        waitsFor(function () {
            return $('a.g-nav-link[g-target="admin"]').length > 0;
        }, 'admin console link to load');
        runs(function () {
            $('a.g-nav-link[g-target="admin"]').click();
        });
        waitsFor(function () {
            return $('.g-plugins-config').length > 0;
        }, 'the admin console to load');
        runs(function () {
            $('.g-plugins-config').click();
        });
        waitsFor(function () {
            return $('input.g-plugin-switch[key="database_assetstore"]').length > 0;
        }, 'the plugins page to load');
        girderTest.waitForLoad();
        runs(function () {
            expect($('.g-plugin-config-link[g-route="plugins/database_assetstore/config"]').length > 0);
            $('.g-plugin-config-link[g-route="plugins/database_assetstore/config"]').click();
        });
        waitsFor(function () {
            return $('#g-database-assetstore-config-form input').length > 0;
        }, 'settings to be shown');
        girderTest.waitForLoad();

        runs(function () {
            $('#g-database-assetstore-user-databases').prop('checked', true);
            $('#g-database-assetstore-pattern').val('^postgresql://.*');
            $('#g-database-assetstore-add').click();
            $('#g-database-assetstore-pattern').val('.*');
            $('#g-database-assetstore-add').click();
            $('.g-database-assetstore-remove').eq(-1).click();
            $('#g-database-assetstore-save').click();
        });
        waitsFor(function () {
            var resp;
            try {
                resp = girder.rest.restRequest({
                    url: 'system/setting',
                    method: 'GET',
                    data: {
                        list: JSON.stringify([
                            'database_assetstore.user_databases',
                            'database_assetstore.user_databases_groups'
                        ])
                    },
                    async: false
                });
            } catch (err) {
                return false;
            }
            if (!resp || !resp.responseJSON) {
                return false;
            }
            var settings = resp.responseJSON;
            var settingsGroups = settings && settings['database_assetstore.user_databases_groups'];
            return (settings['database_assetstore.user_databases'] === true &&
                    settingsGroups && settingsGroups.length === 1 &&
                    settingsGroups[0].pattern === '^postgresql://.*');
        }, 'settings to change');
        runs(function () {
            $('#g-database-assetstore-add').click();
            expect($('#g-database-assetstore-error-message').text().indexOf('must be') >= 0).toBe(true);
            $('#g-database-assetstore-cancel').click();
        });
        waitsFor(function () {
            return $('input.g-plugin-switch[key="database_assetstore"]').length > 0;
        }, 'the plugins page to load');
        girderTest.waitForLoad();
        runs(function () {
            $('.g-plugin-config-link[g-route="plugins/database_assetstore/config"]').click();
        });
        waitsFor(function () {
            return $('#g-database-assetstore-config-form input').length > 0;
        }, 'settings to be shown');
        girderTest.waitForLoad();
        runs(function () {
            expect($('.g-database-assetstore-remove').length).toBe(1);
        });
    });
});

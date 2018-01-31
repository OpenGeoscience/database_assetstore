girderTest.importPlugin('database_assetstore');

girderTest.startApp();

describe('Test upload widget', function () {
    it('change the settings to allow uploads', function () {
        girderTest.login('admin', 'Admin', 'Admin', 'adminpassword')();
        runs(function () {
            girder.rest.restRequest({
                url: 'system/setting',
                method: 'PUT',
                data: {list: JSON.stringify([{
                    key: 'database_assetstore.user_databases',
                    value: true
                }, {
                    key: 'database_assetstore.user_databases_groups',
                    value: [{groupId: '', pattern: '^sqlite://.*'}]
                }])},
                async: false
            });
        });
        girderTest.logout('logout from admin')();
    });
    it('go to a user folder', function () {
        girderTest.login('user1', 'First', 'User', 'userpassword')();
        runs(function () {
            $('.g-my-folders').click();
        });
        waitsFor(function () {
            return $('li.g-folder-list-entry').length > 0;
        }, 'the new folder to display in the list');
        girderTest.waitForLoad();
        runs(function () {
            $('a.g-folder-list-link:last').click();
        });
        waitsFor(function () {
            return $('.g-list-checkbox').length === 0 &&
                   $('.g-empty-parent-message').length === 1;
        }, 'Public folder to be visible');
        girderTest.waitForLoad();
    });
    it('upload a table', function () {
        runs(function () {
            $('.g-upload-here-button').click();
        });
        girderTest.waitForDialog();
        runs(function () {
            expect($('.g-import-database').length).toBe(1);
            $('.g-import-database').click();
        });
        waitsFor(function () {
            return $('#g-import-controls').length > 0;
        }, 'database upload controls to be shown');
        runs(function () {
            $('#g-edit-dbas-dburi').val('sqlite:////user/user1/Public/Sqlite/Sqlite').trigger('change');
        });
        waitsFor(function () {
            return $('#g-dbas-table-name option').length > 0;
        }, 'database tables to be listed');
        runs(function () {
            $('#g-edit-dbas-dburi').val('sqlite:////user/user1/Public/nosuchfile').trigger('change');
        });
        waitsFor(function () {
            return !$('#g-dbas-table-name option').length;
        }, 'database tables to no longer be listed');
        runs(function () {
            $('#g-edit-dbas-dburi').val('sqlite:////user/user1/Public/Sqlite/Sqlite').trigger('change');
        });
        waitsFor(function () {
            return $('#g-dbas-table-name option').length > 0;
        }, 'database tables to be listed');
        runs(function () {
            $('.g-start-import').click();
            expect($('.g-validation-failed-message').text().indexOf('Select at least one')).toBe(0);
            $('#g-dbas-table-name').val($('#g-dbas-table-name option:contains("iris")').val());
            $('#g-dbas-sort-param').val('nosuchcolumn');
            $('.g-start-import').click();
        });
        waitsFor(function () {
            return $('.g-validation-failed-message').text().indexOf('Sort must use') === 0;
        }, 'failure message');
        runs(function () {
            $('#g-dbas-sort-param').val('');
            $('.g-start-import').click();
        });
        girderTest.waitForLoad();
        runs(function () {
            expect($('.g-item-list-link:contains("iris")').length).toBe(1);
        });
        // test that a regular upload still works after having uploaded a table
        girderTest.testUpload(100);
    });
});

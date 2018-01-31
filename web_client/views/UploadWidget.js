import _ from 'underscore';

import UploadWidget from 'girder/views/widgets/UploadWidget';
import { wrap } from 'girder/utilities/PluginUtils';
import { restRequest } from 'girder/rest';

import UploadWidgetTemplate from '../templates/uploadWidget.pug';
import TableSelectTemplate from '../templates/tableSelect.pug';
import '../stylesheets/uploadWidget.styl';

import 'girder/utilities/jquery/girderEnable';

/**
 * Given the current control's URI value, get the list of known tables and
 * update the selection control.
 */
function getTableList() {
    var uri = this.$('#g-edit-dbas-dburi').val().trim();
    // if no change, we don't need to do anything
    if (uri === this._lastUri) {
        return;
    }
    // if we are currently trying to get the table list, abort that request,
    // ask anew via a debounced call
    if (this._listTableXHR && this._listTableXHR.abort) {
        this._listTableXHR.abort();
        this._listTableXHR = null;
        this._debounceGetTableList();
        return;
    }
    this._lastUri = uri;
    this._listTableXHR = restRequest({
        url: 'database_assetstore/user/tables',
        data: {uri},
        error: null
    }).done((resp) => {
        this.tableList = resp;
        this.$('#g-dbas-table-select-container').html(TableSelectTemplate({
            multiple: this.multiFile,
            tableList: resp,
            maxSelectSize: 10
        }));
        this._listTableXHR = null;
        this.$('.g-start-import').girderEnable(true);
    }).fail((resp, xhr) => {
        if (xhr.status === 0) { // aborted request, do nothing
            return;
        }
        this.$('#g-dbas-table-name').empty();
        this.tableList = undefined;
        this._listTableXHR = null;
        this.$('.g-start-import').girderEnable(false);
    });
}

/**
 * Get values from the form and try to import the table.  If we succeed, close
 * the dialog.  If we fail, report it.
 */
function importTable() {
    this.$('.g-validation-failed-message').empty();
    var tableValue = this.$('#g-dbas-table-name').val() || [],
        tables = [], i, value, entry;
    for (i = 0; i < tableValue.length; i += 1) {
        value = tableValue[i];
        entry = '';
        if (value.indexOf('database:') === 0) {
            entry = {};
            value = value.substr(9); // remove 'database:'
            if (value.indexOf(':table:') >= 0) {
                entry['name'] = value.substr(value.indexOf(':table:') + 7);
                value = value.substr(0, value.indexOf(':table:'));
            }
            entry['database'] = value;
        }
        tables.push(entry);
    }
    if (!tables.length) {
        this.$('.g-validation-failed-message').text('Select at least one table');
        return;
    }
    var params = {
        uri: this.$('#g-edit-dbas-dburi').val().trim(),
        table: JSON.stringify(tables),
        sort: this.$('#g-dbas-sort-param').val(),
        fields: this.$('#g-dbas-fields-param').val(),
        filters: this.$('#g-dbas-filters-param').val(),
        limit: this.$('#g-dbas-limit-param').val(),
        format: this.$('#g-dbas-format-param').val(),
        parentType: this.parentType,
        parentId: this.parent.id,
        progress: true,
        replace: false
    };
    this.$('.g-start-import,#g-edit-dbas-dburi').girderEnable(false);
    restRequest({
        url: 'database_assetstore/user/import',
        type: 'PUT',
        data: params,
        error: null
    }).done(() => {
        this.trigger('g:imported');
        $('.modal').girderModal('close');
        this.trigger('g:uploadFinished', {
            // the files entry is currently only used by Girder to update the
            // count of files in the parent, so it should be relatively safe to
            // pass our table list
            files: tables,
            totalSize: 0
        });
    }).fail((err) => {
        this.trigger('g:error', err);
        this.$('.g-validation-failed-message').text(err.responseJSON.message);
        this.$('.g-start-import,#g-edit-dbas-dburi').girderEnable(true);
    });
}

wrap(UploadWidget, 'initialize', function (initialize) {
    initialize.apply(this, Array.prototype.slice.call(arguments, 1));

    // check if this user is allowed to import a database
    restRequest({
        url: 'database_assetstore/user/import/allowed'
    }).done((resp) => {
        if (this.dbImportAllowed === undefined) {
            this.dbImportAllowed = resp;
            this.render();
        }
    });
    return this;
});

wrap(UploadWidget, 'render', function (render) {
    render.apply(this, Array.prototype.slice.call(arguments, 1));

    if (this.dbImportAllowedRendered === undefined && this.dbImportAllowed !== undefined) {
        this.dbImportAllowedRendered = true;
        if (this.dbImportAllowed) {
            this.$('.g-drop-zone').after(UploadWidgetTemplate({}));
            this.events['click .g-import-database'] = function () {
                // convert to the import dialog
                this.$('#g-import-controls').siblings().remove();
                this.$('#g-import-controls').removeClass('hide');
                this.$('.g-start-upload').removeClass('g-start-upload').addClass('g-start-import').text('Import');
                // TODO: add button to search for existing sqlite files
                this._debounceGetTableList = _.bind(_.debounce(getTableList, 100), this);
                this.events['change #g-edit-dbas-dburi'] = this._debounceGetTableList;
                this.events['input #g-edit-dbas-dburi'] = this._debounceGetTableList;
                // note that this is not an arrow function, as we want `this`
                // to be from the event caller's context, not the current
                // context.
                this.events['submit #g-upload-form'] = function (e) {
                    e.preventDefault();
                    if (this.$('.g-start-import').length) {
                        importTable.call(this, e);
                    } else {
                        this.startUpload();
                    }
                };
                this.delegateEvents();
            };
            this.delegateEvents();
        }
    }
    return this;
});

wrap(UploadWidget, 'filesChanged', function (filesChanged) {
    this.dbImportAllowed = false;
    $('.g-import-database').remove();
    filesChanged.apply(this, Array.prototype.slice.call(arguments, 1));
});

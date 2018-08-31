import _ from 'underscore';

import EditFileWidget from 'girder/views/widgets/EditFileWidget';
import AssetstoreModel from 'girder/models/AssetstoreModel';
import { AssetstoreType } from 'girder/constants';
import { wrap } from 'girder/utilities/PluginUtils';
import { restRequest } from 'girder/rest';

import AssetstoreEditFileTemplate from '../templates/dbAssetstoreEditFile.pug';
import TableSelectTemplate from '../templates/tableSelect.pug';

const DB_INFO_KEY = 'databaseMetadata';

// TODO: combine duplicate code with Upload widget
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
            multiple: false,
            tableList: resp,
            maxSelectSize: 10
        }));
        this._listTableXHR = null;
        this.$('.g-save-file').girderEnable(true);
    }).fail((resp, xhr) => {
        if (xhr.status === 0) { // aborted request, do nothing
            return;
        }
        this.$('#g-dbas-table-name').empty();
        this.tableList = undefined;
        this._listTableXHR = null;
        this.$('.g-save-file').girderEnable(false);
    });
    return this._listTableXHR;
}

/**
 * Adds Database-specific operations to the edit dialog.
 */
wrap(EditFileWidget, 'initialize', function (initialize) {
    initialize.apply(this, Array.prototype.slice.call(arguments, 1));
    if (this.file.get(DB_INFO_KEY)) {
        var assetstore = new AssetstoreModel({_id: this.file.get('assetstoreId')});
        assetstore.once('g:fetched', () => {
            this.assetstore = assetstore;
            this.render();
        }).fetch();
    }
});

var fieldsMap = {
    '#g-edit-dbas-dburi': 'uri',
    '#g-dbas-sort-param': 'sort',
    '#g-dbas-fields-param': 'fields',
    '#g-dbas-filters-param': 'filters',
    '#g-dbas-limit-param': 'limit',
    '#g-dbas-format-param': 'format'
};

/**
 * Adds Database-specific operations to the edit dialog.
 */
wrap(EditFileWidget, 'render', function (render) {
    const dbInfo = this.file.get(DB_INFO_KEY);
    if (dbInfo && !this.assetstore) {
        return this;
    }
    render.call(this);
    if (!dbInfo || this.assetstore.get('type') !== AssetstoreType.DATABASE) {
        return this;
    }
    this.$('.g-validation-failed-message').before(
        AssetstoreEditFileTemplate({
            assetstore: this.assetstore,
            file: this.model
        })
    );
    Object.keys(fieldsMap).forEach((key) => {
        this.$(key).val(dbInfo[fieldsMap[key]]);
    });
    this._debounceGetTableList = _.bind(_.debounce(getTableList, 100), this);
    getTableList.call(this).then(() => {
        this.$('#g-dbas-table-name').val('database:' + dbInfo['database'] + ':table:' + dbInfo['table']);
        return null;
    });
    this.events['change #g-edit-dbas-dburi'] = this._debounceGetTableList;
    this.events['input #g-edit-dbas-dburi'] = this._debounceGetTableList;
    // TODO: override 'submit #g-file-edit-form' event
    // TODO: make sure PUT file accepts all database updates
    this.delegateEvents();
    return this;
});

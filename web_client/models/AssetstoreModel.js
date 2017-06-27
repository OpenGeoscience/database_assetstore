import AssetstoreModel from 'girder/models/AssetstoreModel';
import { restRequest } from 'girder/rest';

/**
 * Extends the core assetstore model to add Database-specific functionality.
 */
AssetstoreModel.prototype.databaseImport = function (params) {
    return restRequest({
        path: 'database_assetstore/' + this.get('_id') + '/import',
        type: 'PUT',
        data: params,
        error: null
    }).done(() => {
        this.trigger('g:imported');
    }).fail((err) => {
        this.trigger('g:error', err);
    });
};

AssetstoreModel.prototype.databaseGetTables = function (params) {
    return restRequest({
        path: 'database_assetstore/' + this.get('_id') + '/tables',
        type: 'GET',
        data: params,
        error: null
    }).done((resp) => {
        this.trigger('g:databaseGetTables', resp);
    }).fail((err) => {
        this.trigger('g:error', err);
    });
};

export default AssetstoreModel;

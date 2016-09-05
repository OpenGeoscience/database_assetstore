/**
 * Extends the core assetstore model to add Database-specific functionality.
 */
(function () {
    var prototype = girder.models.AssetstoreModel.prototype;

    prototype.databaseImport = function (params) {
        girder.restRequest({
            path: 'database_assetstore/' + this.get('_id') + '/import',
            type: 'PUT',
            data: params,
            error: null
        }).done(_.bind(function () {
            this.trigger('g:imported');
        }, this)).error(_.bind(function (err) {
            this.trigger('g:error', err);
        }, this));

        return this;
    };

    prototype.databaseGetTables = function (params) {
        girder.restRequest({
            path: 'database_assetstore/' + this.get('_id') + '/tables',
            type: 'GET',
            data: params,
            error: null
        }).done(_.bind(function (resp) {
            this.trigger('g:databaseGetTables', resp);
        }, this)).error(_.bind(function (err) {
            this.trigger('g:error', err);
        }, this));

        return this;
    };
})();

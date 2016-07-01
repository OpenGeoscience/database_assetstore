girder.AssetstoreType.DATABASE = 'database';

/**
 * Adds Database-specific info and an import button to the assetstore list
 * view.
 */
girder.wrap(girder.views.AssetstoresView, 'render', function (render) {
    render.call(this);

    var selector = '.g-assetstore-info-section[assetstore-type="' +
        girder.AssetstoreType.DATABASE + '"]';

    _.each(this.$(selector), function (el) {
        var $el = $(el),
            assetstore = this.collection.get($el.attr('cid'));
        $el.append(girder.templates.db_assetstore_info({
            assetstore: assetstore
        }));
        $el.parent().find('.g-assetstore-buttons').append(
            girder.templates.db_assetstore_importButton({
                assetstore: assetstore
            })
        );
    }, this);

    this.$('.g-dbas-import-button').tooltip({
        delay: 200
    });
});

/**
 * Add UI for creating new Database assetstore.
 */
girder.wrap(girder.views.NewAssetstoreWidget, 'render', function (render) {
    render.call(this);

    this.$('#g-assetstore-accordion').append(girder.templates.db_assetstore_create());
});

girder.views.NewAssetstoreWidget.prototype.events['submit #g-new-dbas-form'] = function (e) {
    this.createAssetstore(e, this.$('#g-new-dbas-error'), {
        type: girder.AssetstoreType.DATABASE,
        name: this.$('#g-new-dbas-name').val(),
        dbtype: this.$('#g-new-dbas-dbtype').val(),
        dburi: this.$('#g-new-dbas-dburi').val()
    });
};

/**
 * Adds Database-specific fields to the edit dialog.
 */
girder.wrap(girder.views.EditAssetstoreWidget, 'render', function (render) {
    render.call(this);

    if (this.model.get('type') === girder.AssetstoreType.DATABASE) {
        this.$('.g-assetstore-form-fields').append(
            girder.templates.db_assetstore_editFields({
                assetstore: this.model
            })
        );
    }
});

girder.views.EditAssetstoreWidget.prototype.fieldsMap.database = {
    get: function () {
        return {
            dbtype: this.$('#g-edit-dbas-dbtype').val(),
            dburi: this.$('#g-edit-dbas-dburi').val()
        };
    },
    set: function () {
        var dbInfo = this.model.get('database');
        this.$('#g-edit-dbas-dbtype').val(dbInfo.dbtype);
        this.$('#g-edit-dbas-dburi').val(dbInfo.uri);
    }
};

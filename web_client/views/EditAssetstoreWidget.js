import EditAssetstoreWidget from 'girder/views/widgets/EditAssetstoreWidget';
import { AssetstoreType } from 'girder/constants';
import { wrap } from 'girder/utilities/PluginUtils';

import AssetstoreEditFieldsTemplate from '../templates/dbAssetstoreEditFields.pug';

/**
 * Adds Database-specific fields to the edit dialog.
 */
wrap(EditAssetstoreWidget, 'render', function (render) {
    render.call(this);

    if (this.model.get('type') === AssetstoreType.DATABASE) {
        this.$('.g-assetstore-form-fields').append(
            AssetstoreEditFieldsTemplate({
                assetstore: this.model
            })
        );
    }
    return this;
});

EditAssetstoreWidget.prototype.fieldsMap.database = {
    get: function () {
        return {
            dburi: this.$('#g-edit-dbas-dburi').val()
        };
    },
    set: function () {
        var dbInfo = this.model.get('database');
        this.$('#g-edit-dbas-dburi').val(dbInfo.uri);
    }
};

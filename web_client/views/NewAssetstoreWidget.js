import NewAssetstoreWidget from 'girder/views/widgets/NewAssetstoreWidget';
import { AssetstoreType } from 'girder/constants';
import { wrap } from 'girder/utilities/PluginUtils';

import AssetstoreCreateTemplate from '../templates/dbAssetstoreCreate.pug';

/**
 * Add UI for creating new Database assetstore.
 */
wrap(NewAssetstoreWidget, 'render', function (render) {
    render.call(this);

    this.$('#g-assetstore-accordion').append(AssetstoreCreateTemplate());
    return this;
});

NewAssetstoreWidget.prototype.events['submit #g-new-dbas-form'] = function (e) {
    this.createAssetstore(e, this.$('#g-new-dbas-error'), {
        type: AssetstoreType.DATABASE,
        name: this.$('#g-new-dbas-name').val(),
        dbtype: this.$('#g-new-dbas-dbtype').val(),
        dburi: this.$('#g-new-dbas-dburi').val()
    });
};

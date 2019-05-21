import _ from 'underscore';

import AssetstoresView from '@girder/core/views/body/AssetstoresView';
import { AssetstoreType } from '@girder/core/constants';
import { wrap } from '@girder/core/utilities/PluginUtils';

import AssetstoreInfoTemplate from '../templates/dbAssetstoreInfo.pug';
import AssetstoreImportButtonTemplate from '../templates/dbAssetstoreImportButton.pug';

/**
 * Adds Database-specific info and an import button to the assetstore list
 * view.
 */
wrap(AssetstoresView, 'render', function (render) {
    render.call(this);

    var selector = '.g-assetstore-info-section[assetstore-type="' + AssetstoreType.DATABASE + '"]';

    _.each(this.$(selector), function (el) {
        var $el = this.$(el),
            assetstore = this.collection.get($el.attr('cid'));
        // don't list the user-authorized assetstore
        if ((assetstore.get('database') || {}).dbtype === 'USER') {
            $el.closest('.g-assetstore-container').remove();
            return;
        }
        $el.append(AssetstoreInfoTemplate({
            assetstore: assetstore
        }));
        $el.parent().find('.g-assetstore-buttons').append(
            AssetstoreImportButtonTemplate({
                assetstore: assetstore
            })
        );
    }, this);

    this.$('.g-dbas-import-button').tooltip({
        delay: 100
    });
    return this;
});

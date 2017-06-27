import router from 'girder/router';
import events from 'girder/events';

import AssetstoreModel from './models/AssetstoreModel';
import AssetstoreImportView from './views/AssetstoreImportView';

router.route('database_assetstore/:id/import', 'dbasImport', function (id) {
    // Fetch the assetstore by id, then render the view.
    var assetstore = new AssetstoreModel({ _id: id });
    assetstore.once('g:fetched', function () {
        events.trigger('g:navigateTo', AssetstoreImportView, {
            model: assetstore
        });
    }).once('g:error', function () {
        router.navigate('assetstores', {trigger: true});
    }).fetch();
});

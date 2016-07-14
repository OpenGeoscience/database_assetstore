$(function () {
    girderTest.addCoveredScripts([
        '/plugins/database_assetstore/web_client/js/AssetstoreModel.js',
        '/plugins/database_assetstore/web_client/js/AssetstoresPage.js',
        '/plugins/database_assetstore/web_client/js/ImportView.js',
        '/static/built/plugins/database_assetstore/templates.js'
    ]);

    girderTest.importStylesheet(
        '/static/built/plugins/database_assetstore/plugin.min.css'
    );

    girder.events.trigger('g:appload.before');
    var app = new girder.App({
        el: 'body',
        parentView: null
    });
    girder.events.trigger('g:appload.after');

    describe('Example test block', function () {
        it('Example test case', function () {
            expect('a').toBe('a');
        });
    });
});

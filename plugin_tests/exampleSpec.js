$(function () {
    girderTest.addCoveredScripts([
    /*
        '/static/built/plugins/database_assetstore/templates.js',
        '/plugins/database_assetstore/web_client/js/imageViewerSelectWidget.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/base.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/geojs.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/leaflet.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/openlayers.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/openseadragon.js',
        '/plugins/database_assetstore/web_client/js/imageViewerWidget/slideatlas.js'
    */
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

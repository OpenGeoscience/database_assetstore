$(function () {
    girderTest.addCoveredScripts([
    /*
        '/static/built/plugins/girder_db_items/templates.js',
        '/plugins/girder_db_items/web_client/js/imageViewerSelectWidget.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/base.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/geojs.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/leaflet.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/openlayers.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/openseadragon.js',
        '/plugins/girder_db_items/web_client/js/imageViewerWidget/slideatlas.js'
    */
    ]);

    girderTest.importStylesheet(
        '/static/built/plugins/girder_db_items/plugin.min.css'
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

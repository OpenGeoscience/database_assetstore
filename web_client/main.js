import events from 'girder/events';
import router from 'girder/router';

import './routes';

// Extends and overrides API
import './constants';
import './views/AssetstoresView';
import './views/EditAssetstoreWidget';
import './views/EditFileWidget';
import './views/NewAssetstoreWidget';
import './views/UploadWidget';

import { exposePluginConfig } from 'girder/utilities/PluginUtils';

import ConfigView from './views/ConfigView';

const pluginName = 'database_assetstore';
const configRoute = `plugins/${pluginName}/config`;

exposePluginConfig(pluginName, configRoute);

router.route(configRoute, 'databaseAssetstoreConfig', function () {
    events.trigger('g:navigateTo', ConfigView);
});

import events from '@girder/core/events';
import router from '@girder/core/router';

import './routes';

// Extends and overrides API
import './constants';
import './views/AssetstoresView';
import './views/EditAssetstoreWidget';
import './views/NewAssetstoreWidget';
import './views/UploadWidget';

import { exposePluginConfig } from '@girder/core/utilities/PluginUtils';

import ConfigView from './views/ConfigView';

const pluginName = 'database_assetstore';
const configRoute = `plugins/${pluginName}/config`;

exposePluginConfig(pluginName, configRoute);

router.route(configRoute, 'databaseAssetstoreConfig', function () {
    events.trigger('g:navigateTo', ConfigView);
});

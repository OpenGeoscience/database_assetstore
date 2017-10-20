import PluginConfigBreadcrumbWidget from 'girder/views/widgets/PluginConfigBreadcrumbWidget';
import View from 'girder/views/View';
import events from 'girder/events';
import router from 'girder/router';
import { restRequest } from 'girder/rest';
import GroupCollection from 'girder/collections/GroupCollection';

import template from '../templates/configView.pug';
import '../stylesheets/configView.styl';

var ConfigView = View.extend({
    events: {
        'click .g-database-assetstore-remove': function (event) {
            this.$('#g-database-assetstore-error-message').text('');
            var index = parseInt($(event.currentTarget).attr('data-index'), 10);
            this.rules.splice(index, 1);
            this.render();
        },
        'click #g-database-assetstore-add': function (event) {
            this.$('#g-database-assetstore-error-message').text('');
            var pattern = $('#g-database-assetstore-pattern').val();
            var group = $('#g-database-assetstore-group').val();
            if (pattern === '') {
                this.$('#g-database-assetstore-error-message').text(
                    'A pattern must be specified.  Use .* to match everything.');
                return;
            }
            var rule = {
                pattern: pattern,
                groupId: group
            };
            this.rules.push(rule);
            this.render();
        },
        'click #g-database-assetstore-save': function (event) {
            this.$('#g-database-assetstore-error-message').text('');
            this._saveSettings([{
                key: 'database_assetstore.user_databases',
                value: this.$('#g-database-assetstore-user-databases').is(':checked')
            }, {
                key: 'database_assetstore.user_databases_groups',
                value: this.rules.length ? this.rules : ''
            }]);
        },
        'click #g-database-assetstore-cancel': function (event) {
            router.navigate('plugins', {trigger: true});
        }
    },

    initialize: function () {
        this.rules = [];

        this.collection = new GroupCollection();
        this.collection.pageLimit = 0;
        this.collection.on('g:changed', function () {
            this.render();
        }, this).fetch();

        this.breadcrumb = new PluginConfigBreadcrumbWidget({
            pluginName: 'Database assetstore',
            parentView: this
        });

        restRequest({
            method: 'GET',
            url: 'system/setting',
            data: {
                list: JSON.stringify([
                    'database_assetstore.user_databases',
                    'database_assetstore.user_databases_groups'
                ])
            }
        }).done((resp) => {
            this.settings = resp;
            this.rules = resp['database_assetstore.user_databases_groups'] || [];
            this.render();
        });
    },

    render: function () {
        var groups = this.collection.toArray();

        var groupsById = {};
        groups.forEach(function (group) {
            groupsById[group.id] = group;
        });
        this.$el.html(template({
            settings: this.settings,
            groups: groups,
            groupsById: groupsById,
            rules: this.rules
        }));
        this.breadcrumb.setElement(this.$('.g-config-breadcrumb-container')).render();
        return this;
    },

    _saveSettings: function (settings) {
        restRequest({
            method: 'PUT',
            url: 'system/setting',
            data: {
                list: JSON.stringify(settings)
            },
            error: null
        }).done(() => {
            events.trigger('g:alert', {
                icon: 'ok',
                text: 'Settings saved.',
                type: 'success',
                timeout: 4000
            });
        }).fail((resp) => {
            this.$('#g-database-assetstore-error-message').text(
                resp.responseJSON.message);
        });
    }
});

export default ConfigView;

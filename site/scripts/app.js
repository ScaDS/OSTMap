/**
 * Created by Christopher Rost on 24.04.2016.
 */

/**
 * The main angular instance with configurations.
 */
(function () {
    'use strict';

    angular.module('ostMapApp', [
            'ngRoute',
            'ui-leaflet',
            'ui.layout'
        ])
        .config(defineRoutes);

    /**
     * Inject dependencies for the defineRoutes configuration
     * $routeProvider to set routes
     * @type {string[]}
     */
    defineRoutes.$inject = [
        '$routeProvider'
    ];

    /**
     * Sets the routes
     * @param $routeProvider
     */
    function defineRoutes($routeProvider) {
        $routeProvider
            .when('/list', {
                templateUrl: 'views/listView.html',
                controller: 'ListCtrl',
                name: 'List',
                glyphicon: 'glyphicon glyphicon-list'
            })
            .when('/map', {
                templateUrl: 'views/mapView.html',
                controller: 'MapCtrl',
                name: 'Map',
                glyphicon: 'glyphicon glyphicon-globe'
            })
            .otherwise({redirectTo: '/list'});
    }

})();
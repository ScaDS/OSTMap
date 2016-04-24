/**
 * Created by Christopher on 24.04.2016.
 */

/**
 * A service to publish all routes for the navigation directive.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .factory('routeNavigation',routeNavigation);

    routeNavigation.$inject = [
        '$route',
        '$location'
    ];

    function routeNavigation($route,$location) {
        var routes = [];
        angular.forEach($route.routes, function (route, path) {
            if (route.name) {
                routes.push({
                    path: path,
                    name: route.name
                });
            }
        });
        return {
            routes: routes,
            activeRoute: function (route) {
                return route.path === $location.path();
            }
        };
    }


})();
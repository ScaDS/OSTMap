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

    /**
     * Inject dependencies for the service
     * $route to access all defined routes
     * $location to determine the current url
     * @type {string[]}
     */
    routeNavigation.$inject = [
        '$route',
        '$location'
    ];

    /**
     * Core Service logic
     * @param $route
     * @param $location
     * @returns {{routes: Array, activeRoute: activeRoute}} Returns all named routes and a function to determine if a route is the current root
     */
    function routeNavigation($route,$location) {
        var routes = [];
        /**
         * Iterates over all defined routes. If there is a rout with a name specified, the properties path name and glyphicon will be saved in the array 'routes'
         */
        angular.forEach($route.routes, function (route, path) {
            if (route.name) {
                routes.push({
                    path: path,
                    name: route.name,
                    glyphicon: route.glyphicon
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
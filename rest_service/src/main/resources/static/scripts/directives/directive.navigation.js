/**
 * Created by Christopher on 24.04.2016.
 */

/**
 * The navigation directive. Use it with <navigation></navigation>. It loads all named routes from $routes and inserts them in the nav bar.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .directive('navigation',navigation);

    /**
     * Inject dependencies for the directive
     * routeNavigation the service to get active routes
     * @type {string[]}
     */
    navigation.$inject = [
        'routeNavigation'
    ];

    /**
     *
     * @param routeNavigation the injected service
     * @returns {{restrict: string, replace: boolean, templateUrl: string, controller: controller}}
     */
    function navigation(routeNavigation) {
        return {
            restrict: "E",
            replace: true,
            templateUrl: "views/partials/navBar.tpl.html",
            controller: function ($scope) {
                //Get routes from the service
                $scope.routesRight = routeNavigation.routesRight;
                $scope.routesLeft = routeNavigation.routesLeft;
                //Get the active route from the service to highlight it at the navbar
                $scope.activeRoute = routeNavigation.activeRoute;
            }
        };
    }


})();
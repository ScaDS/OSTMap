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

    navigation.$inject = [
        'routeNavigation'
    ];

    function navigation(routeNavigation) {
        return {
            restrict: "E",
            replace: true,
            templateUrl: "views/partials/navBar.tpl.html",
            controller: function ($scope) {
                $scope.routes = routeNavigation.routes;
                $scope.activeRoute = routeNavigation.activeRoute;
            }
        };
    }


})();
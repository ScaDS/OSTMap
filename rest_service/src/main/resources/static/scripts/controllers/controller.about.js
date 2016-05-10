/**
 * The controller for the list view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('AboutCtrl', AboutCtrl);

    /**
     * Inject all dependencies for the controller
     * $scope to interact with the view
     * @type {string[]}
     */
    AboutCtrl.$inject = [
        '$scope'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function AboutCtrl($scope) {
        $scope.github = [];
        $scope.github.url = "http://github.com/IIDP/OSTMap"
    }

})();
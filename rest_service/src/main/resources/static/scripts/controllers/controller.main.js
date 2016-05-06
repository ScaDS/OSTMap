/**
 * Created by Christopher on 24.04.2016.
 */

/**
 * The Main controller instance. Valid for the whole app and all pages.
 */

(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('MainCtrl',MainCtrl);

    /**
     * Inject dependencies for the controller
     * $scope to interact with the view
     * $location (not used yet)
     * @type {string[]}
     */
    MainCtrl.$inject = [
        '$scope',
        '$location'
        ];

    /**
     * The controller logic
     *
     * @param $scope
     * @param $location
     * @constructor
     */
    function MainCtrl($scope,$location) {

        $scope.app = [];
        $scope.app.name = "OSTMap";
    
    }
})();
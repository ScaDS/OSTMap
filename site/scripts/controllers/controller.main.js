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

    MainCtrl.$inject = [
        '$scope',
        '$location'
        ];

    function MainCtrl($scope,$location) {

        $scope.app = [];
        $scope.app.name = "OSTMap";
    
    }
})();
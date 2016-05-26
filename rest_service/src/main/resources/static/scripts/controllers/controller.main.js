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
        '$rootScope',
        '$scope',
        '$interval',
        'httpService'
        ];

    /**
     * The controller logic
     *
     * @param $scope
     * @param $location
     * @constructor
     */
    function MainCtrl($rootScope, $scope, $interval, httpService) {
        
        $scope.app = [];
        $scope.app.name = "OSTMap";

        document.getElementById("loading").style.visibility = "hidden";
        
        $scope.timePassed = 0;
        $scope.info = "";
        var timestamp = 0;

        $scope.alerts = [
            // { type: 'danger', msg: 'Oh snap! Change a few things up and try submitting again.' },
            // { type: 'success', msg: 'Well done! You successfully read this important alert message.' }
        ];

        $scope.addAlert = function() {
            $scope.alerts.push({msg: 'Another alert!'});
        };

        $scope.closeAlert = function(index) {
            $scope.alerts.splice(index, 1);
        };

        $scope.ignoreLoading = function () {
            httpService.setLoading(false);
            $scope.$emit('updateStatus', status);
            // document.getElementById("loading").style.visibility = "hidden";
        }

        $scope.$on('updateStatus', function(event, message){
            if (message != 200) {
                $scope.info = message;
            }
            $scope.setLoadingDisplay(httpService.getLoading(), message);
        });

        $scope.setLoadingDisplay = function (loadingStatus, message) {
            if (loadingStatus) {
                document.getElementById("loading").style.visibility = "visible";
                // $scope.alerts.push({type: 'danger', msg: message + " " + $scope.timePassed});
            } else {
                document.getElementById("loading").style.visibility = "hidden";
                // $scope.alerts.splice(index, 1);
            }

            timestamp = Date.now()
            var intervalPromise = $interval(function () {
                $scope.timePassed = Math.round((Date.now() - timestamp)/100)/10;

                if(!httpService.getLoading()) {
                    $interval.cancel(intervalPromise);
                }
            }, 100);
        };

        $rootScope.$on('alertControl', function(event, message){
            console.log("alertControl triggered");
            if (message != 200) {
                $scope.info = message;
            }
            $scope.setLoadingDisplay(httpService.getLoading(), message);
        });
    }
})();
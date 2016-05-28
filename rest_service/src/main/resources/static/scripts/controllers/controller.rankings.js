/**
 * The controller for the analytics view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('RankingsCtrl', RankingsCtrl);

    /**
     * Inject all dependencies for the controller
     * $scope to interact with the view
     * @type {string[]}
     */
    RankingsCtrl.$inject = [
        '$scope',
        'httpService',
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function RankingsCtrl($scope, httpService) {
        // httpService.getHighScoreFromServer();
        // $scope.data.highScore = httpService.getHighScore();
        $scope.data = []
        $scope.data.highScore = {"area":[{"user":"Zorne","area":6174893.727618582,"coordinates":[[-0.2147884,-0.4614716],[27.2147884,38.4614716],[49,26.546974]]},{"user":"Falk","area":2465661.8600413124,"coordinates":[[0,0],[10,10],[10,20],[20,0]]},{"user":"Oliver","area":54980.275141825,"coordinates":[[-2.2147884,52.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-8.2147884,53.4614716]]}],"path":[{"user":"Zorne","distance":7589.900654023497,"coordinates":[[49,26.546974],[27.2147884,38.4614716],[-0.2147884,-0.4614716]]},{"user":"Falk","distance":5818.530021620169,"coordinates":[[0,0],[20,0],[10,20],[10,10],[10,10],[10,10],[10,10]]},{"user":"Peter","distance":4925.676750216901,"coordinates":[[-3.2147884,53.4614716],[47.2147884,28.4614716]]},{"user":"Oliver","distance":1062.1904270002783,"coordinates":[[-3.2147884,53.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-2.2147884,52.4614716],[-8.2147884,53.4614716]]}]}


        $scope.goToHighScoreArea = function() {
            //TODO: move map to highscore, render area
        }
        $scope.goToHighScoreDistance = function() {
            //TODO: move map to highscore, render path
        }

    }
})();
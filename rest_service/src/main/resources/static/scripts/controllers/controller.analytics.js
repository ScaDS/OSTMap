/**
 * The controller for the analytics view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('AnalyticsCtrl', AnalyticsCtrl);

    /**
     * Inject all dependencies for the controller
     * $scope to interact with the view
     * @type {string[]}
     */
    AnalyticsCtrl.$inject = [
        '$scope'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function AnalyticsCtrl($scope) {
        $scope.options = {
            renderer: 'area'
        };

        $scope.features = {
            xAxis: {
                timeUnit: 'hour'
            },
            yAxis: {
                tickFormat: 'formatKMBT'
            },
            hover: {
                xFormatter: function(x) {
                    return 't=' + x;
                },
                yFormatter: function(y) {
                    return '$' + y;
                }
            },
            legend: {
                toggle: true,
                highlight: true
            }
        };

        $scope.series = [{
            name: 'Series 1',
            color: 'steelblue',
            data: [{x: 0, y: 23}, {x: 1, y: 15}, {x: 2, y: 79}, {x: 3, y: 31}, {x: 4, y: 60}]
        }, {
            name: 'Series 2',
            color: 'lightblue',
            data: [{x: 0, y: 30}, {x: 1, y: 20}, {x: 2, y: 64}, {x: 3, y: 50}, {x: 4, y: 15}]
        }];
    }
})();
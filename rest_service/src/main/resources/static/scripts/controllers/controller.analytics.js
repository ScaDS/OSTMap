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
        '$scope',
        'httpService'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function AnalyticsCtrl($scope, httpService) {

        $scope.timeFilter = 24;
        $scope.data = {
            "start": 0,
            "end": 0,
            "series": [
                {
                    name: 'Series 1',
                    color: 'steelblue',
                    data: [{x: 0, y: 23}, {x: 1, y: 15}, {x: 2, y: 79}, {x: 3, y: 31}, {x: 4, y: 60}]
                },
                {
                    name: 'Series 2',
                    color: 'lightblue',
                    data: [{x: 0, y: 30}, {x: 1, y: 20}, {x: 2, y: 64}, {x: 3, y: 50}, {x: 4, y: 15}]
                }
            ]
        };

        $scope.data.tweetFrequency = httpService.getTweetFrequency();

        /**
         * Update filters
         */
        var updateQueued = false;
        $scope.updateFilters = function () {
            if (!httpService.getLoading()) {
                console.log("Filters updated: " + $scope.timeFilter + "h");
                httpService.setLoading(true);
                /**
                 * get the tweets from the REST interface
                 */
                httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter($scope.timeFilter)).then(function (status) {
                    $scope.$emit('updateStatus', status);
                    // $scope.data.tweetFrequency = httpService.getTweetFrequency();
                    $scope.populateMap();
                });
            } else {
                updateQueued = true;
            }
        };

        $scope.$on('updateStatus', function(event, message){
            if(updateQueued) {
                $scope.updateFilters();
                updateQueued = false;
            }
        });

        $scope.populateMap = function () {
            console.log("Data: " + $scope.data);
            //TODO: update map with data


        };


        /**
         * Interpret the time filter and return a time window
         * @returns {number[]}
         */
        function parseTimeFilter(hours){
            var times = [0, 0];
            var start;
            var end;
            var date = new Date();
            var currentTime = date.getTime()/1000; //milliseconds to seconds

            var offset = 60*60*hours;

            if (hours == 0) {
                times[0] = 0;
            } else {
                start = Math.round(currentTime - offset);
            }
            end = Math.round(currentTime);

            times[0] = start;
            times[1] = end;

            $scope.data.start = start;
            $scope.data.end = end;

            return times;
        }

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
    }
})();
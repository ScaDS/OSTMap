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
        'httpService',
        '$interval'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function AnalyticsCtrl($scope, httpService, $interval) {
        $scope.autoUpdateEnabled = true;
        $scope.timeFilter = 1;
        // var testseries = [];
        // for (var i=0; i<46; i++){
        //     testseries.push(
        //         {
        //             name: 'de',
        //             // name: 'Series ' + i,
        //             color: 'steelblue',
        //             data: []
        //         }
        //     )
        // }

        $scope.data = {
            "start": 0,
            "end": 0,
            "series": [{
                name: 'default',
                color: 'steelblue',
                data: []
            }]
        };

        // $scope.graph = new Rickshaw.Graph( {
        //     element: angular.element.find("chart"),
        //     width: 580,
        //     height: 250,
        //     series: testseries
        // } );
        //
        // $scope.graph.render();

        /**
         * Update filters
         */
        var updateQueued = false;
        $scope.updateFilters = function () {
            if (!httpService.getLoading()) {
                $scope.data.series = [{
                    name: 'default',
                    color: 'steelblue',
                    data: []
                }];

                console.log("Filters updated: " + $scope.timeFilter + "h");
                httpService.setLoading(true);
                /**
                 * get the tweets from the REST interface
                 */
                httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter($scope.timeFilter)).then(function (status) {
                    $scope.$emit('updateStatus', status);
                    $scope.data.raw = httpService.getTweetFrequency();
                    $scope.populateChart();
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

        $scope.populateChart = function () {
            var langIndex = 0;
            console.log("Languages: " + Object.keys($scope.data.raw).length);

            var start = $scope.data.start/1000/60;
            var end = $scope.data.end/1000/60;
            var range = end - start + 1;
            // if (data.hasOwnProperty("en")) {
            //     console.log("has data")
            //     range = data.en.length;
            // }

            /**
             * Iterate over all languages, fill series
             */
            for (var lang in $scope.data.raw) {
                if ($scope.data.raw.hasOwnProperty(lang)) {
                    // console.log("Range calculated and received: " + lang + "[" + range + "] " + lang + "[" + data[lang].length + "]");
                    // if ($scope.data.series[langIndex].hasOwnProperty(lang)) {
                    //     // $scope.data.series[langIndex]
                    // }
                    if ($scope.data.series[langIndex] == undefined) {
                        $scope.data.series[langIndex] = (
                            {
                                name: lang,
                                color: 'steelblue',
                                data: []
                            }
                        );
                    } else {
                        console.log("already exists " + langIndex + " " + $scope.data.series[langIndex].name + " == " + lang)
                    }
                    var points = [];
                    // for (var i = 0; i < range; i++) {
                    for (var i = 0; i < range; i++) {
                        var point = {
                            // x: start + i,
                            x: i,
                            y: $scope.data.raw[lang][i]
                        };
                        // points.push(point);
                        $scope.data.series[langIndex].data.push(point);
                    }

                    // console.log(series);
                    // $scope.data.series.push(series);
                    langIndex++;
                }
            }
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
            var currentTime = new Date().getTime();

            // console.log("Current time: " + new Date().toDateString());

            var offset = 1000*60*60*hours; //milliseconds, seconds, minutes

            // console.log("offset: " + offset)

            start = currentTime - offset;
            end = currentTime;

            $scope.data.start = start;
            $scope.data.end = end;

            date.setTime(start);
            // console.log("start:\n" + date);
            start = zeroPad(date.getUTCFullYear(), 4)+
                    zeroPad(date.getUTCMonth()+1 , 2)+
                    zeroPad(date.getUTCDate()    , 2)+
                    zeroPad(date.getUTCHours()   , 2)+
                    zeroPad(date.getUTCMinutes() , 2);
            console.log("start:\n" + start);

            date.setTime(end);
            // console.log("end:\n" + date);
            end =   zeroPad(date.getUTCFullYear(), 4)+
                    zeroPad(date.getUTCMonth()+1 , 2)+
                    zeroPad(date.getUTCDate()    , 2)+
                    zeroPad(date.getUTCHours()   , 2)+
                    zeroPad(date.getUTCMinutes() , 2);
            console.log("end:\n" + end);

            times[0] = start;
            times[1] = end;

            return times;
        }

        function zeroPad(num, places) {
            var padded = "0";
            // console.log("pad before: " + num)
            var zero = places - num.toString().length + 1;
            padded = Array(+(zero > 0 && zero)).join("0").toString() + num;
            // console.log("pad after: " + padded)
            return padded;
        }

        $scope.options = {
            renderer: 'area'
        };

        $scope.features = {
            xAxis: {
                timeUnit: 'minute'
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

        /**
         * Run when page is loaded
         */
        $scope.$on('$viewContentLoaded', function() {
            $scope.data.raw = httpService.getTweetFrequency();
            if (Object.keys($scope.data.raw).length > 0) {
                console.log("Existing Data: " + Object.keys($scope.data.raw).length);
                $scope.populateChart();

                //TODO: catchup function in autoUpdate (missing between last update and now)
                if ($scope.autoUpdateEnabled) {
                    $scope.autoUpdate();
                }
            } else if ($scope.autoUpdateEnabled) {
                httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter($scope.timeFilter)).then(function (status) {
                    $scope.$emit('updateStatus', status);
                    $scope.data.raw = httpService.getTweetFrequency();
                    $scope.populateChart();
                    if ($scope.autoUpdateEnabled) {
                        $scope.autoUpdate();
                    }
                });
            }
        });

        $scope.autoUpdate = function () {
            var updateFrequency = 1/60; //in hours
            var intervalPromise = $interval(function () {
                if(!$scope.autoUpdateEnabled) {
                    $interval.cancel(intervalPromise);
                } else {
                    console.log("Doing AutoUpdate: " + $scope.autoUpdateEnabled);

                    httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter(updateFrequency)).then(function (status) {

                        $scope.$emit('updateStatus', status);
                        $scope.data.raw = httpService.getTweetFrequency();
                        $scope.populateChart();
                    });
                }
            }, 1000*60*60*updateFrequency);
        };
    }
})();
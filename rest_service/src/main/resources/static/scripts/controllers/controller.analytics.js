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
        $scope.timeFilter = 60;
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
            "range": 0,
            "oldSize": 0,
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
         * Rickshaw color palette
         *
         * COLOR SCHEMES
         * - classic9
         * - colorwheel
         * - cool
         * - munin
         * - spectrum14
         * - spectrum2000
         * - spectrum2001
         *
         * @type {Rickshaw.Color.Palette}
         */
        var palette = new Rickshaw.Color.Palette( { scheme: 'spectrum2000' } );

        
        /**
         * Update filters
         */
        var updateQueued = false;
        $scope.updateFilters = function () {
            if (!httpService.getLoading()) {
                var temp = $scope.autoUpdateEnabled;
                $scope.autoUpdateEnabled = false;       //temporarily disable autoUpdate

                console.log("Filters updated: " + $scope.timeFilter/60 + "h");
                clearLocalData();
                httpService.setLoading(true);
                /**
                 * get the tweets from the REST interface
                 */
                httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter($scope.timeFilter)).then(function (status) {
                    $scope.$emit('updateStatus', status);
                    $scope.data.raw = httpService.getTweetFrequency();
                    $scope.populateChart()

                    $scope.autoUpdateEnabled = temp;
                    $scope.autoUpdate();
                });

                $scope.$emit('updateStatus', "Loading Tweet Frequency: " + $scope.timeFilter + "min");
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

        function clearLocalData() {
            for (var langIndex = 0; langIndex < $scope.data.series.length; langIndex++) {
                $scope.data.series[langIndex].data.splice(0,$scope.data.oldSize);
            }
            $scope.data.oldSize = 0;
        }

        $scope.populateChart = function () {
            console.log("Languages: " + Object.keys($scope.data.raw).length);
            console.log("Old data size: " + $scope.data.oldSize);
            console.log("Amount of new data: " + $scope.data.range);

            // if($scope.data.start == 0){
            //     $scope.data.start = $scope.data.startUnixMinutes;
            //     $scope.data.end = $scope.data.endUnixMinutes;
            // } else {
            //     clearLocalData();
            // }

            // console.log("start: " + $scope.data.start);
            // console.log("end: " + $scope.data.end);
            // console.log("range: " + $scope.data.range);

            /**
             * Create a new series filled with 0s if the lang does not exist
             */
            for (var lang in $scope.data.raw) {
                if($scope.data.series.filter(function(object){return object.name == lang;}).length == 0){
                    var newSeries = {
                        name: lang,
                        color: palette.color(),
                        data: []
                    };
                    for (var i = 0; i < $scope.data.oldSize; i++) {
                        newSeries.data.push({
                            x: i,
                            y: 0
                        });
                    }
                    $scope.data.series.push(newSeries);
                }
            }
            /**
             * Fill existing series' (all should exist at this point) to match the number of points
             * If no data for the specific series is available, fill with zeros
             */
            for (var langIndex = 0; langIndex < $scope.data.series.length; langIndex++) {   //iterate all series
                var lang = $scope.data.series[langIndex].name;                              //get series language

                if ($scope.data.raw.hasOwnProperty(lang)) {                                 //check if the new data has the selected language
                    for (var time = 0; time < $scope.data.range; time++) {
                        $scope.data.series[langIndex].data.push({
                            x: time + $scope.data.oldSize,
                            y: $scope.data.raw[lang][time]
                        });
                    }
                } else {
                    for (var time = 0; time < $scope.data.range; time++) {
                        $scope.data.series[langIndex].data.push({
                            x: time + $scope.data.oldSize,
                            y: 0
                        });
                    }
                }
                // console.log(lang + ":" + Object.keys($scope.data.series[langIndex].data).length);
            }
            $scope.data.oldSize += $scope.data.range;

            //Why? Don't ask me why. It just works.
            for (var langIndex = 0; langIndex < $scope.data.series.length; langIndex++) {
                var name = $scope.data.series[langIndex].name;
                var color = $scope.data.series[langIndex].color;
                var data = $scope.data.series[langIndex].data;
                $scope.data.series[langIndex] = {
                    name: name,
                    color: color,
                    data: data
                };
            }
        };


        /**
         * Interpret the time filter and return a time window
         * @returns {number[]}
         */
        function parseTimeFilter(minutes){
            var times = [0, 0];
            var start;
            var end;
            var date = new Date();
            var currentTime = new Date().getTime();
            var twoMinutes = 1000*60*2; //delay is needed. 1 minute so the minute can finish. another minute for the server to process

            // console.log("Current time: " + new Date().toDateString());

            var offset = 1000*60*minutes; //milliseconds, seconds, minutes

            // console.log("offset: " + offset)

            start = (currentTime - offset) - twoMinutes;
            end = currentTime - twoMinutes;

            $scope.data.startUnixMilliseconds = start;
            $scope.data.startUnixMinutes = Math.floor((start/1000)/60);
            $scope.data.endUnixMilliseconds = end;
            $scope.data.endUnixMinutes = Math.floor((end/1000)/60);
            $scope.data.range = ($scope.data.endUnixMinutes - $scope.data.startUnixMinutes) + 1; //+1 for inclusive range

            date.setTime(start);
            // console.log("start:\n" + date);
            start = zeroPad(date.getUTCFullYear(), 4)+
                    zeroPad(date.getUTCMonth()+1 , 2)+
                    zeroPad(date.getUTCDate()    , 2)+
                    zeroPad(date.getUTCHours()   , 2)+
                    zeroPad(date.getUTCMinutes() , 2);
            // console.log("start:\n" + start);

            date.setTime(end);
            // console.log("end:\n" + date);
            end =   zeroPad(date.getUTCFullYear(), 4)+
                    zeroPad(date.getUTCMonth()+1 , 2)+
                    zeroPad(date.getUTCDate()    , 2)+
                    zeroPad(date.getUTCHours()   , 2)+
                    zeroPad(date.getUTCMinutes() , 2);
            // console.log("end:\n" + end);

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
            console.log("autoUpdate: " + $scope.autoUpdateEnabled);

            var updateFrequency = 60; //in seconds
            var count = 0;
            var intervalPromise = $interval(function () {
                if(!$scope.autoUpdateEnabled) {
                    $interval.cancel(intervalPromise);
                } else {
                    count++;
                    console.log("Doing AutoUpdate: " + count);

                    httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter(0)).then(function (status) {

                        $scope.$emit('updateStatus', status);
                        $scope.data.raw = httpService.getTweetFrequency();
                        $scope.populateChart();
                    });
                }
            }, 1000*updateFrequency);
        };
    }
})();
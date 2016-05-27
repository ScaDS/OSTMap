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
        $scope.autoUpdateEnabled = false;
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
                console.log("Filters updated: " + $scope.timeFilter/60 + "h");
                httpService.setLoading(true);
                /**
                 * get the tweets from the REST interface
                 */
                httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter($scope.timeFilter)).then(function (status) {
                    $scope.$emit('updateStatus', status);
                    $scope.data.raw = httpService.getTweetFrequency();
                    $scope.populateChart();
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
            $scope.data.series = [{
                name: 'default',
                color: 'steelblue',
                data: []
            }];
            $scope.data.xSize = 0;
        }

        $scope.populateChart = function () {
            console.log("Languages: " + Object.keys($scope.data.raw).length);

            if($scope.data.start == 0){
                $scope.data.start = $scope.data.startUnixMinutes;
                $scope.data.end = $scope.data.endUnixMinutes;
            } else if ($scope.data.endUnixMinutes > $scope.data.end) {
                $scope.data.end = $scope.data.endUnixMinutes;
            } else if ($scope.data.startUnixMinutes < $scope.data.start) {
                clearLocalData();
            }

            // console.log("start: " + $scope.data.start);
            // console.log("end: " + $scope.data.end);
            // console.log("range: " + $scope.data.range);

            /**
             * Create a new empty series if the lang does not exist
             */
            for (var lang in $scope.data.raw) {
                if($scope.data.range != Object.keys($scope.data.raw[lang]).length){
                    console.log("Array length mismatch!!!")
                } else {
                    if($scope.data.series.filter(function(object){return object.name == lang;}).length == 0){
                        var newSeries = {
                            name: lang,
                            color: palette.color(),
                            data: []
                        };
                        $scope.data.series.push(newSeries);
                    }
                }
            }
            /**
             * Fill existing series (all should exist at this point) to match the number of points
             * If no data for the specific series is available, fill with zeros
             */
            for (var langIndex = 0; langIndex < $scope.data.series.length; langIndex++) {   //iterate all series
                var lang = $scope.data.series[langIndex].name;                              //get series language
                var target = $scope.data.series[langIndex].data;

                if ($scope.data.raw.hasOwnProperty(lang)) {                                     //check if the new data has the selected language
                    var source = $scope.data.raw[lang];

                    for (var time = $scope.data.start; time <= $scope.data.end; time++) {       //iterate through entire time range, start to end
                        if (target.filter(function(time){return time.x == time}) == "") {  //check if point is missing
                            if (time < $scope.data.startUnixMinutes) {                          //check if undefined point is before new data range i.e. fill gaps in the data
                                //fill with zeros
                                $scope.data.series[langIndex].data.push({
                                    x: time,
                                    y: 0
                                });
                            } else {
                                //fill with new data
                                $scope.data.series[langIndex].data.push({
                                    x: time,
                                    y: source[time - $scope.data.startUnixMinutes]
                                });
                            }
                        }
                    }
                } else {
                    for (var time = $scope.data.start; time <= $scope.data.end; time++) {       //iterate through entire time range, start to end
                        if (target.filter(function(time){return time.x == time}) == "") {  //check if point is missing
                            //fill with zeros
                            $scope.data.series[langIndex].data.push({
                                x: time,
                                y: 0
                            });
                        }
                    }
                }
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

            var updateFrequency = 1; //in minutes
            var count = 0;
            var intervalPromise = $interval(function () {
                if(!$scope.autoUpdateEnabled) {
                    $interval.cancel(intervalPromise);
                } else {
                    count++;
                    console.log("Doing AutoUpdate: " + count);

                    httpService.getTweetsFromServerByTweetFrequency(parseTimeFilter(updateFrequency)).then(function (status) {

                        $scope.$emit('updateStatus', status);
                        $scope.data.raw = httpService.getTweetFrequency();
                        $scope.populateChart();
                    });
                }
            }, 1000*60*updateFrequency);
        };
    }
})();
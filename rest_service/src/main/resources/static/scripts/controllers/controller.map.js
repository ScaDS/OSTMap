/**
 * Created by Christopher on 24.04.2016.
 */

/**
 * The controller for the map view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('MapCtrl', MapCtrl);

    MapCtrl.$inject = [
        '$scope',
        'httpService',
        '$log',
        'nemSimpleLogger',
        'leafletData',
        '$interval'
    ];

    /**
     * Controller Logic:
     *
     *
     * @param $scope
     * @param httpService
     * @param $log
     * @param nemSimpleLogger
     * @param leafletData
     * @constructor
     */
    function MapCtrl($scope, httpService, $log, nemSimpleLogger, leafletData, $interval) {
        mapInit($scope);

        $scope.autoUpdate = false;
        $scope.dataSource = "localhost"; //default: "accumulo";
        $scope.clusteringEnabled = true;
        $scope.usePruneCluster = true;

        $scope.currentFilters = "";
        $scope.timeFilter = 0.25;
        $scope.search = [];
        $scope.hashtagFilter = "";

        $scope.data = [];
        $scope.data = httpService.getTweetsGeo();
        if (!$scope.data.hasOwnProperty('tweets')) {
            $scope.data.tweets = [];
        }
        if (!$scope.data.hasOwnProperty('top10')) {
            $scope.data.top10 = [];
        }

        /**
         * Reset all filter values to default or null
         */
        $scope.search.clearFilters = function () {
            $scope.timeFilter = "0.25";
            $scope.center ={
                lat: 50,
                lng: 12,
                zoom: 4
            };

            setTimeout(function(){
                $scope.search.updateFilters();
            }, 500);
        };

        /**
         * Set the hashtag filter by clicking on a top10 hashtag then call a filter update
         * @param hashtag
         */
        $scope.filterBy = function (hashtag) {
            if ($scope.hashtagFilter == hashtag) {
                $scope.hashtagFilter = "";
                $scope.populateMarkers();
            } else {
                $scope.hashtagFilter = hashtag;

                $scope.filteredData = [];

                for (var i = 0; i < $scope.data.tweets.length; i++) {
                    if ($scope.data.tweets[i].text.indexOf(hashtag) !== -1) {
                        $scope.filteredData.push($scope.data.tweets[i]);
                    }
                }

                console.log($scope.filteredData.length + " Tweets with Hashtag: " + hashtag)

                $scope.populateMarkers($scope.filteredData);
            }
        };

        /**
         * Update filters
         */
        var updateQueued = false;
        $scope.search.updateFilters = function () {
            if (!httpService.getLoading()) {
                httpService.setLoading(true);
                /**
                 * Pass the filters to the httpService
                 */
                httpService.setTimeWindow(parseTimeFilter());
                httpService.setBoundingBox($scope.getBounds());
                
                /**
                 * get the tweets from the REST interface
                 */
                if ($scope.dataSource == "accumulo") {
                    //Get by GeoTime
                    httpService.getTweetsFromServerByGeoTime().then(function (status) {
                        doUpdate();
                    });
                } else if ($scope.dataSource == "localhost") {
                    //Get using test REST API
                    httpService.getTweetsFromServerTest().then(function (status) {
                        doUpdate();
                    });
                } else if ($scope.dataSource == "static") {
                    //Get from local (debug)
                    httpService.getTweetsFromLocal().then(function (status) {
                        doUpdate();
                    });
                }

                var doUpdate = function () {
                    // $scope.$emit('updateStatus', status);
                    $scope.data = httpService.getTweetsGeo();
                    $scope.populateMarkers();
                };

                /**
                 * Update the filter display
                 * Check for null values, replace with Default
                 *
                 * @type {string}
                 */
                $scope.currentFilters = $scope.timeFilter + "h | " +
                    "[" + httpService.getBoundingBox().bbnorth.toFixed(2) +
                    ", " + httpService.getBoundingBox().bbwest.toFixed(2) +
                    ", " + httpService.getBoundingBox().bbsouth.toFixed(2) +
                    ", " + httpService.getBoundingBox().bbeast.toFixed(2) + "]";

                console.log("Filters updated: " + $scope.currentFilters + " | " + $scope.bounds);
                $scope.$emit('updateStatus', "Loading: " + $scope.currentFilters + " | " + $scope.bounds);
            } else {
                updateQueued = true;
            }
        };

        $scope.$on('updateStatus', function(event, message){
            if(updateQueued) {
                $scope.search.updateFilters();
                updateQueued = false;
            }
        });

        /**
         * Populate the map with markers using coordinates from each tweet
         * Ignore tweets without coordinates
         */
        $scope.populateMarkers = function (sourceData) {
            if (sourceData === undefined) {
                sourceData = $scope.data.tweets;
                $scope.hashtagFilter = "";
            }

            /**
             * Reset all markers
             */
            $scope.markers = {};
            var newMarkers = [];
            $scope.pruneMarkers = [];
            $scope.pruneCluster.RemoveMarkers();
            $scope.pruneCluster.ProcessView();

            /**
             * Iterate through tweets
             * Filter bad data
             * Add coordinate pairs to marker array
             */
            sourceData.forEach( function(tweet) {
                if($scope.markers[tweet.id_str] == undefined && tweet.coordinates != null) {
                    /**
                     * Create new marker then add to marker array
                     * @type {{id_str: *, lat: *, lng: *, focus: boolean, draggable: boolean, message: *, icon: {}}}
                     */
                    var tweetMessage = "Missing  tweet.user.screen_name and/or tweet.id_str";

                    if(tweet.user.hasOwnProperty('screen_name') && tweet.hasOwnProperty('id_str')){
                        tweetMessage = '<iframe id="tweet_' + tweet.id_str + '" class="Tweet" frameborder=0 src="http://twitframe.com/show?url=https%3A%2F%2Ftwitter.com%2F' + tweet.user.screen_name +  '%2Fstatus%2F' + tweet.id_str + '"></iframe>';
                    }

                    var newMarker = {
                        id_str: tweet.id_str,
                        lat: tweet.coordinates.coordinates[1],
                        lng: tweet.coordinates.coordinates[0],
                        focus: false,
                        draggable: false,
                        message: tweetMessage,
                        opacity: 0.40
                    };

                    if($scope.clusteringEnabled) {
                        if ($scope.usePruneCluster) {
                            var pruneMarker = new PruneCluster.Marker(tweet.coordinates.coordinates[1], tweet.coordinates.coordinates[0]);
                            pruneMarker.data = newMarker;
                            pruneMarker.data.icon = L.divIcon($scope.icons.red);
                            pruneMarker.data.popup = tweetMessage;

                            $scope.pruneCluster.RegisterMarker(pruneMarker);
                            $scope.pruneMarkers.push(pruneMarker);
                        } else {
                            newMarker.layer = 'cluster';
                            newMarker.icon = $scope.icons.red;
                            newMarkers.push(newMarker);
                        }
                    } else {
                        newMarker.layer = 'dots';
                        newMarker.icon = $scope.icons.red;
                        newMarkers.push(newMarker);
                    }
                }
            });

            $scope.pruneCluster.ProcessView();
            $scope.markers = newMarkers;

            /**
             * Manual ui-leaflet marker creation
             * See $scope.markersWatchOptions for watcher toggles
             * Also: HTML: markers-watch-options="markersWatchOptions"
             */
            // leafletData.getDirectiveControls().then(function (controls) {
            //     controls.markers.create({});
            //     controls.markers.create(newMarkers);
            //     console.log("Marker population done in: " + (Date.now() - startTime) + "ms");
            // });
        };

        $scope.currentBounds = null;

        /**
         * Return bounds as object.
         *
         * @returns {{bbnorth: *, bbwest: *, bbsouth: *, bbeast: *}}
         */
        $scope.getBounds = function () {
            var north, west, south, east;

            if ($scope.currentBounds._northEast.lat >= 90) {north = 89.99}
            else {north = $scope.currentBounds._northEast.lat}

            if ($scope.currentBounds._southWest.lng <= -180) {west = -179.99}
            else {west = $scope.currentBounds._southWest.lng}

            if ($scope.currentBounds._southWest.lat <= -90) {south = -89.99}
            else {south = $scope.currentBounds._southWest.lat}

            if ($scope.currentBounds._northEast.lng >= 180) {east = 179.99}
            else {east = $scope.currentBounds._northEast.lng}

            return {
                bbnorth: north,
                bbwest: west,
                bbsouth: south,
                bbeast: east
            };
        };

        /**
         * Interpret the time filter and return a time window
         * @returns {number[]}
         */
        function parseTimeFilter(){
            var times = [0, 0];
            var date = new Date();
            var currentTime = date.getTime()/1000; //milliseconds to seconds

            var hours = $scope.timeFilter;
            var offset = 60*60*hours;

            if (hours == 0) {
                times[0] = 0;
            } else {
                times[0] = Math.round(currentTime - offset);
            }
            times[1] = Math.round(currentTime);

            return times;
        }

        $scope.liveUpdates = function() {
            console.log("live: " + $scope.timeFilter);
            var updateInterval = _.clone($scope.timeFilter);

            var count = 0;
            var intervalPromise = $interval(function () {
                if($scope.timeFilter != updateInterval) {
                    $interval.cancel(intervalPromise);
                } else {
                    count++;
                    console.log("Doing AutoUpdate: " + count);
                    httpService.setTimeWindow(parseTimeFilter());

                    httpService.getTweetsFromServerTest().then(function () {
                        //Variant A: New Tweets get added to current Tweets
                        $scope.data.tweets.push.apply(httpService.getTweetsGeo().tweets);
                        $scope.data.top10.push.apply(httpService.getTweetsGeo().top10);

                        //Variant B: Old Tweets deleted, new ones shown instead
                        // $scope.data = httpService.getTweetsGeo();

                        $scope.populateMarkers();
                    });
                }
            }, 1000*60*60*updateInterval);
        };

        /**
         * Run when page is loaded
         */
        $scope.$on('$viewContentLoaded', function() {
            // console.log("Page Loaded");
            $scope.onStart();
        });

        /**
         * Run-once
         * Update the filters when the bounds are changed
         * Adds PruneCluster
         */
        $scope.onStart = function () {
            leafletData.getMap("geoTemporalMap").then(function(map) {
                $scope.pruneCluster = new PruneClusterForLeaflet();
                map.addLayer($scope.pruneCluster);

                map.on('moveend', function() {
                    $scope.currentBounds = map.getBounds();
                    if($scope.autoUpdate) {
                        // console.log("Map watcher triggered, updating filters");
                        $scope.search.updateFilters();
                    } else {
                        // console.log("Map watcher triggered, autoUpdateDisabled: no action taken");
                    }
                });
                // console.log("Mapbounds watcher started");

                /**
                 * Workaround to trigger a filter update due to a mapbound change
                 * @type {{lat: number, lng: number, zoom: number}}
                 */
                $scope.center ={
                    lat: 50,
                    lng: 12,
                    zoom: 5
                };

                /**
                 * If data was already fetched previously, load it
                 */
                if ($scope.data.hasOwnProperty('tweets') && $scope.data.tweets.length > 0) {
                    console.log("Existing Data: " + $scope.data.tweets.length);
                    $scope.populateMarkers();
                }
            });
        };
    }

    /**
     * Map Logic
     * angular-ui / ui-leaflet
     * https://github.com/angular-ui/ui-leaflet
     *
     * @param $scope
     */
    function mapInit($scope) {
        /**
         * default coordinates for ui-leaflet map
         * @type {{lat: number, lng: number, zoom: number}}
         */
        $scope.center ={
            lat: 50,
            lng: 12,
            zoom: 4
        };
        $scope.regions = {
            europe: {
                northEast: {
                    lat: 70,
                    lng: 40
                },
                southWest: {
                    lat: 35,
                    lng: -25
                }
            }
        };
        $scope.maxBounds = {
            northEast: {
                lat: 90,
                lng: 180
            },
            southWest: {
                lat: -90,
                lng: -180
            }
        };
        $scope.bounds = null;

        /**
         * Marker icon definition
         * @type {{blue: {type: string, iconSize: number[], className: string, iconAnchor: number[]}, red: {type: string, iconSize: number[], className: string, iconAnchor: number[]}}}
         */
        $scope.icons = {
            blue: {
                type: 'div',
                iconSize: [11, 11],
                className: 'blue',
                iconAnchor:  [6, 6]
            },
            red: {
                type: 'div',
                iconSize: [11, 11],
                className: 'red',
                iconAnchor:  [6, 6]
            },
            smallerDefault: {
                iconUrl: 'bower_components/leaflet/dist/images/marker-icon.png',
                // shadowUrl: 'bower_components/leaflet/dist/images/marker-shadow.png',
                iconSize:     [12, 20], // size of the icon
                // shadowSize:   [25, 41], // size of the shadow
                iconAnchor:   [6, 20], // point of the icon which will correspond to marker's location
                // shadowAnchor: [4, 62],  // the same for the shadow
                popupAnchor:  [0, -18] // point from which the popup should open relative to the iconAnchor
            }
        };

        /**
         * Test markers
         * @type {*[]}
         */
        $scope.markers = {};
        /**
         * Variable used to track the selected marker
         * @type {number}
         */
        $scope.currentMarkerID = 0;

        $scope.events = {
            map: {
                enable: ['moveend', 'popupopen'],
                logic: 'emit'
            },
            marker: {
                enable: [],
                logic: 'emit'
            }
        };

        $scope.layers = {
            baselayers: {
                osm: {
                    name: "OpenStreetMap",
                    type: "xyz",
                    url: "http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                },
                gray: {
                    name: "OpenStreetMap-Gray",
                    type: "xyz",
                    url: "http://{s}.tiles.wmflabs.org/bw-mapnik/{z}/{x}/{y}.png"
                }
            },
            overlays: {
                cluster: {
                    name: "Clustered Markers",
                    type: "markercluster",
                    visible: true,
                    layerOptions: {
                        "chunkedLoading": true,
                        "showCoverageOnHover": false,
                        "removeOutsideVisibleBounds": true,
                        "chunkProgress": updateProgressBar
                    }
                },
                dots: {
                    name: "Red Dots",
                    type: "group",
                    visible: true,
                    layerOptions: {
                        "chunkedLoading": true,
                        "showCoverageOnHover": false,
                        "removeOutsideVisibleBounds": true,
                        "chunkProgress": updateProgressBar
                    }
                }
            }
        };

        $scope.markersWatchOptions = {
            doWatch: false,
            isDeep: false,
            individual: {
                doWatch: false,
                isDeep: false
            }
        };

        function updateProgressBar(processed, total, elapsed, layersArray) {
           // console.log("Chunk loading: " + processed + "/" + total + " " + elapsed + "ms")
        }
    }
})();
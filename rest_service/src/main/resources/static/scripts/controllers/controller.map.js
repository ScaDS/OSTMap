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
        'leafletData'
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
    function MapCtrl($scope, httpService, $log, nemSimpleLogger, leafletData, $q) {
        mapInit($scope);

        $scope.autoUpdate = false;
        $scope.dataSource = "localhost"; //default: "accumulo";
        $scope.clusteringEnabled = true;
        $scope.usePruneCluster = true;

        $scope.currentFilters = "";
        $scope.timeFilter = 0.25;
        $scope.search = [];
        $scope.search.hashtagFilter = "#";
        // $scope.search.searchFilter = "Default Search Filter";
        $scope.search.searchFilter = httpService.getSearchToken();

        $scope.data = [];
        $scope.data.tweets = httpService.getTweets();

        /**
         * Reset all filter values to default or null
         */
        $scope.search.clearFilters = function () {
            // $scope.search.searchFilter = null;
            $scope.search.searchFilter = "DefaultSearchFilter";
            // $scope.timeFilter = null;
            $scope.timeFilter = "0.25";
            $scope.search.hashtagFilter = "#";
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
        $scope.search.setHashtagFilter = function (hashtag) {
            $scope.search.hashtagFilter = "#" + hashtag;
            $scope.search.searchFilter = hashtag;
            $scope.search.updateFilters();
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
                httpService.setSearchToken($scope.search.searchFilter);
                httpService.setTimeWindow(parseTimeFilter());
                httpService.setBoundingBox($scope.getBounds());
                /**
                 * get the tweets from the REST interface
                 */
                httpService.queueAddGetTweetFrom($scope.dataSource, $scope.search);

                if ($scope.dataSource == "accumulo") {
                    //Get by GeoTime
                    httpService.getTweetsFromServerByGeoTime().then(function (status) {
                        doUpdate();
                    });
                } else if ($scope.dataSource == "restTest") {
                    //Get using test REST API
                    httpService.getTweetsFromServerTest().then(function (status) {
                        doUpdate();
                    });
                } else if ($scope.dataSource == "localhost") {
                    //Get using test REST API
                    httpService.getTweetsFromServerTest2().then(function (status) {
                        doUpdate();
                    });
                } else if ($scope.dataSource == "static") {
                    //Get from local (debug)
                    httpService.getTweetsFromLocal().then(function (status) {
                        doUpdate();
                    });
                } else {
                    //Get by Token
                    httpService.getTweetsFromServerByToken().then(function (status) {
                        doUpdate();
                    });
                }

                var doUpdate = function () {
                    $scope.$emit('updateStatus', status);
                    $scope.data.tweets = httpService.getTweets();
                    $scope.populateMarkers();
                };

                /**
                 * Update the filter display
                 * Check for null values, replace with Default
                 *
                 * @type {string}
                 */
                $scope.currentFilters = $scope.search.searchFilter + " | " +
                    $scope.search.hashtagFilter + " | " +
                    $scope.timeFilter + "h | " +
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
        $scope.populateMarkers = function () {
            // console.log("Populating Markers ");
            var startTime = Date.now();

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
            $scope.data.tweets.forEach( function(tweet) {
                // Check if tweet has the property 'coordinates' and 'id_str'... if not, leave the forEach function
                if(!tweet.hasOwnProperty('coordinates') || !tweet.hasOwnProperty('id_str')){
                    console.log("JSON Error");
                    return;
                }

                if($scope.markers[tweet.id_str] == undefined && tweet.coordinates != null) {
                    /**
                     * Create new marker then add to marker array
                     * @type {{id_str: *, lat: *, lng: *, focus: boolean, draggable: boolean, message: *, icon: {}}}
                     */
                    var tweetMessage = '<iframe class="Tweet" frameborder=0 src="http://twitframe.com/show?url=https%3A%2F%2Ftwitter.com%2F' + tweet.user.screen_name +  '%2Fstatus%2F' + tweet.id_str + '"></iframe>'

                    var newMarker = {
                        id_str: tweet.id_str,
                        lat: tweet.coordinates.coordinates[1],
                        lng: tweet.coordinates.coordinates[0],
                        focus: false,
                        draggable: false,
                        // message: "@" + tweet.user.screen_name + ": " + tweet.text,
                        message: tweetMessage,
                        opacity: 0.40
                    };

                    if($scope.clusteringEnabled) {
                        if ($scope.usePruneCluster) {
                            var pruneMarker = new PruneCluster.Marker(tweet.coordinates.coordinates[1], tweet.coordinates.coordinates[0]);
                            pruneMarker.data = newMarker;
                            pruneMarker.data.icon = L.divIcon($scope.icons.red);
                            pruneMarker.data.popup = tweetMessage;
                            // pruneMarker.openPopup = function() {
                            //     L.marker.openPopup;
                            // }

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
            }
            );

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

        /**
         * Move the map center to the coordinates of the clicked tweet
         *
         * @param id_str
         * @param lat
         * @param lng
         */
        $scope.search.goToTweet = function (index, id_str, lat, lng) {
            console.log("selected tweet index: " + index + ", [" + lat + "," + lng + "]");

            /**
             * Check if latitude and longitude are available
             */
            if (lat == undefined || lng == undefined) {
                alert("Missing Coordinates!");
            } else {
                /**
                 * Move map center to the tweet
                 * @type {{lat: *, lng: *, zoom: number}}
                 */
                // $scope.center ={
                //     lat: lat,
                //     lng: lng,
                //     zoom: 10
                // };

                /**
                 * Scroll document to the map element
                 */
                // document.getElementById("map").scrollIntoView();
                document.getElementById("navbar").scrollIntoView();

                /**
                 * Un-selects the old marker
                 * Update currentMarkerID
                 * Give focus to selected tweet
                 * Makes the text label visible
                 */
                if($scope.usePruneCluster) {
                    // $scope.pruneCluster.PrepareLeafletMarker = function(leafletMarker, data) {
                    //     leafletMarker.openPopup();
                    // };

                    if ($scope.currentMarkerID != 0) {
                        // $scope.pruneMarkers[$scope.currentMarkerID].closePopup();
                    }
                    $scope.currentMarkerID = index;

                    if ($scope.pruneMarkers[index] != null) {
                        // $scope.pruneMarkers[index].openPopup();
                        console.log($scope.pruneCluster.GetMarkers()[index]);
                        $scope.pruneCluster.GetMarkers()[index].bindPopup("test");
                        
                        $scope.pruneCluster.PrepareLeafletMarker = function(leafletMarker, data) {
                            leafletMarker.setIcon(L.divIcon($scope.icons.red)); // See http://leafletjs.com/reference.html#icon
                            //listeners can be applied to markers in this function
                            // leafletMarker.on('popup' + data.id_str, function(){
                                //do click event logic here
                                // leafletMarker.openPopup();
                            // });
                            // A popup can already be attached to the marker
                            // bindPopup can override it, but it's faster to update the content instead
                            if (leafletMarker.getPopup()) {
                                leafletMarker.setPopupContent(data.message);
                            } else {
                                leafletMarker.bindPopup(data.message);
                            }
                        };
                    }
                } else {
                    if ($scope.currentMarkerID != 0) {
                        $scope.markers[$scope.currentMarkerID].focus = false;
                    }
                    $scope.currentMarkerID = index;

                    if ($scope.markers[index] != null) {
                        $scope.markers[index].focus = true;
                    }
                }
            }
        };

        $scope.currentBounds = null;

        /**
         * Return bounds as object.
         *
         * @returns {{bbnorth: *, bbwest: *, bbsouth: *, bbeast: *}}
         */
        $scope.getBounds = function () {
            var north, west, south, east;

            if ($scope.currentBounds._northEast.lat > 90) {north = 90}
            else {north = $scope.currentBounds._northEast.lat}

            if ($scope.currentBounds._southWest.lng < -180) {west = -180}
            else {west = $scope.currentBounds._southWest.lng}

            if ($scope.currentBounds._southWest.lat < -90) {south = -90}
            else {south = $scope.currentBounds._southWest.lat}

            if ($scope.currentBounds._northEast.lng > 180) {east = 180}
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

        /**
         * Run when page is loaded
         */
        $scope.$on('$viewContentLoaded', function() {
            console.log("Page Loaded");
            $scope.onStart()
        });

        /**
         * Run-once
         * Update the filters when the bounds are changed
         * Adds PruneCluster
         */
        $scope.onStart = function () {
            leafletData.getMap("map").then(function(map) {
                $scope.pruneCluster = new PruneClusterForLeaflet();
                map.addLayer($scope.pruneCluster);

                // $scope.pruneCluster.PrepareLeafletMarker = function(leafletMarker, data) {
                //     leafletMarker.setIcon(L.divIcon($scope.icons.red)); // See http://leafletjs.com/reference.html#icon
                //     //listeners can be applied to markers in this function
                //     // leafletMarker.on('popup' + data.id_str, function(){
                //         //do click event logic here
                //         // leafletMarker.openPopup();
                //     // });
                //     // A popup can already be attached to the marker
                //     // bindPopup can override it, but it's faster to update the content instead
                //     if (leafletMarker.getPopup()) {
                //         leafletMarker.setPopupContent(data.message);
                //     } else {
                //         leafletMarker.bindPopup(data.message);
                //     }
                // };


                map.on('moveend', function() {
                    $scope.currentBounds = map.getBounds();
                    if($scope.autoUpdate) {
                        // console.log("Map watcher triggered, updating filters");
                        $scope.search.updateFilters();
                    } else {
                        // console.log("Map watcher triggered, autoUpdateDisabled: no action taken");
                    }
                });
                console.log("Mapbounds watcher started");

                /**
                 * Workaround to trigger a filter update due to a mapbound change
                 * @type {{lat: number, lng: number, zoom: number}}
                 */
                $scope.center ={
                    lat: 50,
                    lng: 12,
                    zoom: 3
                };
            });
        };

        /**
         * Populate markers whenever tweet data changes
         */
        // $scope.$watch(function() {
        //         return $scope.data.tweets;
        //     }, function() {
        //         console.log("Data watcher triggered, populating markers");
        //         $scope.populateMarkers();
        //     },
        //     true
        // );

        /**
         * Pagination
         * https://angular-ui.github.io/bootstrap/#/pagination
         */
        $scope.totalItems = 64;
        $scope.currentPage = 4;
        $scope.setPage = function (pageNo) {
            $scope.currentPage = pageNo;
        };
        $scope.pageChanged = function() {
            $log.log('Page changed to: ' + $scope.currentPage);
        };
        $scope.maxSize = 5;
        $scope.bigTotalItems = 175;
        $scope.bigCurrentPage = 1;
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
                iconAnchor:  [6, 6],
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
                gray: {
                    name: "OpenStreetMap-Gray",
                    type: "xyz",
                    url: "http://{s}.tiles.wmflabs.org/bw-mapnik/{z}/{x}/{y}.png"
                },
                osm: {
                    name: "OpenStreetMap",
                    type: "xyz",
                    url: "http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
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
           console.log("Chunk loading: " + processed + "/" + total + " " + elapsed + "ms")
        }
    }
})();
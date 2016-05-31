/**
 * The controller for the list view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('ListCtrl', ListCtrl);

    /**
     * Inject all dependencies for the controller
     * $scope to interact with the view
     * httpService to access the factory
     * @type {string[]}
     */
    ListCtrl.$inject = [
        '$scope',
        '$location',
        'httpService',
        '$log',
        'nemSimpleLogger',
        'leafletData'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @param $location
     * @param httpService
     * @constructor
     */
    function ListCtrl($scope, $location, httpService, $log, nemSimpleLogger, leafletData, $q) {
        mapInit($scope);

        $scope.autoUpdate = false;
        $scope.dataSource = "localhost"; //default: "accumulo";
        $scope.clusteringEnabled = true;
        $scope.usePruneCluster = true;

        $scope.currentFilters = "";
        $scope.search = [];
        $scope.data = [];
        $scope.data.tweets = httpService.getTweets();

        $scope.search.inputValue = httpService.getSearchToken();
        $scope.search.searchFields = httpService.getSearchFields();


        var updateQueued = false;
        $scope.search.updateFilters = function () {
            if (!httpService.getLoading()) {
                /**
                 * Reset the map center
                 * @type {{lat: number, lng: number, zoom: number}}
                 */
                $scope.center ={
                    lat: 50,
                    lng: 12,
                    zoom: 4
                };


                httpService.setLoading(true);
                /**
                 * Pass the filters to the httpService
                 */
                httpService.setSearchToken($scope.search.inputValue);
                httpService.setSearchFields($scope.search.searchFields);

                /**
                 * get the tweets from the REST interface
                 */
                if ($scope.dataSource == "accumulo") {
                    httpService.getTweetsFromServerByToken().then(function (status) {
                        $scope.populateMarkers();
                    });
                } else if ($scope.dataSource == "localhost") {
                    httpService.getTweetsFromServerByToken2().then(function (status) {
                        // $scope.$emit('updateStatus', status);
                        // $scope.data = httpService.getTweets();
                        $scope.populateMarkers();
                    });
                } else if ($scope.dataSource == "static") {
                    httpService.getTweetsFromLocal().then(function (status) {
                        $scope.populateMarkers();
                    });
                }
            } else {
                updateQueued = true;
            }
            $scope.$emit('updateStatus', "Loading: " + $scope.search.searchFields.text.checked + " | " + $scope.search.searchFields.user.checked + " | '" + $scope.search.inputValue + "'");
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
                if ($scope.markers[tweet.id_str] == undefined && tweet.coordinates != null) {
                    /**
                     * Create new marker then add to marker array
                     * @type {{id_str: *, lat: *, lng: *, focus: boolean, draggable: boolean, message: *, icon: {}}}
                     */
                    var tweetMessage = "Missing  tweet.user.screen_name and/or tweet.id_str";

                    if (tweet.user.hasOwnProperty('screen_name') && tweet.hasOwnProperty('id_str')) {
                        tweetMessage = '<iframe id="tweet_' + tweet.id_str + '" class="Tweet" frameborder=0 src="http://twitframe.com/show?url=https%3A%2F%2Ftwitter.com%2F' + tweet.user.screen_name + '%2Fstatus%2F' + tweet.id_str + '"></iframe>';
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

                    if ($scope.clusteringEnabled) {
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
            $scope.totalItems = $scope.data.tweets.length;
            $scope.setPage(1);
        };

        /**
         * Move the map center to the coordinates of the clicked tweet
         *
         * @param index
         * @param id_str
         * @param lat
         * @param lng
         */
        $scope.search.goToTweet = function (index, id_str, lat, lng) {
            console.log("selected tweet index: " + index + ", [" + lat + "," + lng + "] | " + id_str);

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
                $scope.center ={
                    lat: lat,
                    lng: lng,
                    zoom: 10
                };

                /**
                 * Scroll document to the map element
                 */
                // document.getElementById("tokenMap").scrollIntoView();
                document.getElementById("navbar").scrollIntoView();

                /**
                 * Un-selects the old marker
                 * Update currentMarkerID
                 * Give focus to selected tweet
                 * Makes the text label visible
                 */
                // if($scope.usePruneCluster) {
                //     // $scope.pruneCluster.PrepareLeafletMarker = function(leafletMarker, data) {
                //     //     leafletMarker.openPopup();
                //     // };
                //
                //     if ($scope.currentMarkerID != 0) {
                //         // $scope.pruneMarkers[$scope.currentMarkerID].closePopup();
                //     }
                //     $scope.currentMarkerID = index;
                //
                //     if ($scope.pruneMarkers[index] != null) {
                //         // $scope.pruneMarkers[index].openPopup();
                //         console.log($scope.pruneCluster.GetMarkers()[index]);
                //         $scope.pruneCluster.GetMarkers()[index].bindPopup("test");
                //
                //         $scope.pruneCluster.PrepareLeafletMarker = function(leafletMarker, data) {
                //             leafletMarker.setIcon(L.divIcon($scope.icons.red)); // See http://leafletjs.com/reference.html#icon
                //             //listeners can be applied to markers in this function
                //             // leafletMarker.on('popup' + data.id_str, function(){
                //             //do click event logic here
                //             // leafletMarker.openPopup();
                //             // });
                //             // A popup can already be attached to the marker
                //             // bindPopup can override it, but it's faster to update the content instead
                //             if (leafletMarker.getPopup()) {
                //                 leafletMarker.setPopupContent(data.message);
                //             } else {
                //                 leafletMarker.bindPopup(data.message);
                //             }
                //         };
                //     }
                // } else {
                //     if ($scope.currentMarkerID != 0) {
                //         $scope.markers[$scope.currentMarkerID].focus = false;
                //     }
                //     $scope.currentMarkerID = index;
                //
                //     if ($scope.markers[index] != null) {
                //         $scope.markers[index].focus = true;
                //     }
                // }
            }
        };

        $scope.stalkUser = function (username) {
            console.log("Stalking: " + username);
            $scope.search.inputValue = username;
            $scope.search.searchFields.text.checked = false;
            $scope.search.searchFields.user.checked = true;

            $scope.search.updateFilters();

        };

        /**
         * Pagination
         * https://angular-ui.github.io/bootstrap/#/pagination
         */

        $scope.currentPage = 1;
        $scope.setPage = function (pageNo) {
            $scope.currentPage = pageNo;
            $scope.currentSlice = $scope.data.tweets.slice(($scope.currentPage-1) * $scope.itemsPerPage,($scope.currentPage * $scope.itemsPerPage));
        };
        $scope.pageChanged = function() {
            $scope.currentSlice = $scope.data.tweets.slice(($scope.currentPage-1) * $scope.itemsPerPage,($scope.currentPage * $scope.itemsPerPage));
        };
        $scope.maxSize = 5;
        $scope.itemsPerPage = 10;

        /**
         * Run-once
         * Update the filters when the bounds are changed
         * Adds PruneCluster
         */
        $scope.$on('$viewContentLoaded', function() {
            leafletData.getMap("tokenMap").then(function(map) {
                $scope.pruneCluster = new PruneClusterForLeaflet();
                map.addLayer($scope.pruneCluster);

                /**
                 * If data was already fetched previously, load it
                 */
                if ($scope.data.hasOwnProperty('tweets') && $scope.data.tweets.length > 0) {
                    console.log("Existing Data: " + $scope.data.tweets.length);
                    $scope.populateMarkers();
                }
            });
        });
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
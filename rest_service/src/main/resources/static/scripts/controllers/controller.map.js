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
    function MapCtrl($scope, httpService, $log, nemSimpleLogger, leafletData) {
        mapInit($scope);

        $scope.currentFilters = "";
        $scope.timeFilter = 1;
        $scope.search = [];
        $scope.search.hashtagFilter = "#";
        // $scope.search.searchFilter = "Default Search Filter";
        $scope.search.searchFilter = httpService.getSearchToken();

        /**
         * Reset all filter values to default or null
         */
        $scope.search.clearFilters = function () {
            // $scope.search.searchFilter = null;
            $scope.search.searchFilter = "DefaultSearchFilter";
            // $scope.timeFilter = null;
            $scope.timeFilter = "0";
            $scope.search.hashtagFilter = "#";
            $scope.center ={
                lat: 50,
                lng: 12,
                zoom: 4
            };

            $scope.search.updateFilters();
        };
        /**
         * Set the hashtag filter by clicking on a top10 hashtag then call a filter update
         * @param hashtag
         */
        $scope.search.setHashtagFilter = function (hashtag) {
            $scope.search.hashtagFilter = "#" + hashtag;
            $scope.search.updateFilters();
        };
        /**
         * Update filters
         */
        $scope.search.updateFilters = function () {
            /**
             * Pass the filters to the httpService
             */
            httpService.setSearchToken($scope.search.searchFilter);
            // httpService.setSearchToken($scope.search.hashtagFilter);
            // httpService.setSearchToken("yolo");
            httpService.setTimeWindow(parseTimeFilter());
            httpService.setBoundingBox($scope.getBounds());
            /**
             * get the tweets from the REST interface
             */
            httpService.getTweetsFromServerByGeoTime();  //Get by GeoTime
            // httpService.getTweetsFromServerTest();       //Get using test REST API
            // httpService.getTweetsFromLocal();            //Get from local (debug)

            // httpService.getTweetsFromServerByToken();    //Get by Token

            /**
             * Call marker population function
             */
            // $scope.populateMarkers();
            /**
             * Update current map boundaries
             */
            // $scope.getBounds();
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
        };
        /**
         * Move the map center to the coordinates of the clicked tweet
         *
         * @param id
         * @param lat
         * @param lng
         */
        $scope.search.goToTweet = function (id, lat, lng) {
            console.log("selected tweet id: " + id + ", [" + lat + "," + lng + "]");

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
                    zoom: 6
                };

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
                if ($scope.currentMarkerID != 0) {
                    $scope.markers[$scope.currentMarkerID].focus = false;
                }
                $scope.currentMarkerID = id;

                if ($scope.markers[id] != null) {
                    $scope.markers[id].focus = true;
                }
            }
        };

        /**
         * Slider
         * https://github.com/angular-slider/angularjs-slider
         *
         * CURRENTLY UNUSED
         *
         * @type {{value: number, options: {ceil: number, floor: number, showTicksValues: boolean, ticksValuesTooltip: $scope.slider_ticks_values_tooltip.options.ticksValuesTooltip}}}
         */
        //Slider with ticks and values and tooltip
        $scope.slider_ticks_values_tooltip = {
            value: 1,
            options: {
                ceil: 5,
                floor: 1,
                showTicksValues: true,
                ticksValuesTooltip: function (v) {
                    return 'Tooltip for ' + v;
                }
            }
        };

        $scope.data = [];
        $scope.data.tweets = httpService.getTweets();

        /**
         * Populate the map with markers using coordinates from each tweet
         * Ignore tweets without coordinates
         */
        $scope.populateMarkers = function () {
            /**
             * Reset all markers
             */
            // $scope.markers = {}

            /**
             * Iterate through tweets
             * Filter bad data
             * Add coordinate pairs to marker array
             */
            angular.forEach($scope.data.tweets, function(value) {
                var tweet = value;
                // Check if tweet has the property 'coordinates' and 'id'... if not, leave the forEach function
                if(!tweet.hasOwnProperty('coordinates') || !tweet.hasOwnProperty('id')){
                    return;
                }

                if($scope.markers[tweet.id] == undefined && tweet.coordinates != null) {
                    /**
                     * Create new marker then add to marker array
                     * @type {{id: *, lat: *, lng: *, focus: boolean, draggable: boolean, message: *, icon: {}}}
                     */
                    var newMarker = {
                        id: tweet.id,
                        lat: tweet.coordinates.coordinates[1],
                        lng: tweet.coordinates.coordinates[0],
                        focus: false,
                        draggable: false,
                        message: tweet.text,
                        icon: $scope.icons.red
                    };
                    // $scope.markers.push(newMarker)
                    // $scope.markers.push(tweet.id + ": " +  newMarker)
                    $scope.markers[tweet.id] = newMarker;
                }
            });
        };


        $scope.currentBounds = null;
        $scope.runOnce = false;

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

            if ($scope.timeFilter == 0) {
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
            $scope.onBounds()
        });

        /**
         * Update the filters when the bounds are changed
         */
        $scope.onBounds = function () {
            leafletData.getMap("map").then(function(map) {
                map.on('moveend', function() {
                    $scope.currentBounds = map.getBounds();
                    $scope.search.updateFilters();
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
        $scope.$watch(function() {
                return $scope.data.tweets;
            }, function() {
                console.log("Data watcher triggered, populating markers");
                $scope.populateMarkers();
            },
            true
        );

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
        // $scope.maxBounds = $scope.regions.europe;
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
                iconSize: [10, 10],
                className: 'blue',
                iconAnchor:  [5, 5]
            },
            red: {
                type: 'div',
                iconSize: [10, 10],
                className: 'red',
                iconAnchor:  [5, 5]
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
        // $scope.markers = [
        //     {
        //         id: 1,
        //         lat: 51.33843,
        //         lng: 12.37866,
        //         focus: true,
        //         draggable: false,
        //         message: "Test Marker 1",
        //         icon: $scope.icons.smallerDefault
        //     },
        //     {
        //         id: 2,
        //         lat: 51.33948,
        //         lng: 12.37637,
        //         focus: false,
        //         draggable: false,
        //         message: "Test Marker 2",
        //         icon: $scope.icons.blue
        //     }
        // ];
        $scope.markers = {
            // 1: {
            //     id: 1,
            //     lat: 51.33843,
            //     lng: 12.37866,
            //     focus: true,
            //     draggable: false,
            //     message: "Test Marker 1",
            //     icon: $scope.icons.smallerDefault
            // },
            // 2: {
            //     id: 2,
            //     lat: 51.33948,
            //     lng: 12.37637,
            //     focus: false,
            //     draggable: false,
            //     message: "Test Marker 2",
            //     icon: $scope.icons.blue
            // }
        };
        $scope.currentMarkerID = 0;

        /**
         * Map event functions for future extensibility (Marker Clustering)
         * https://asmaloney.com/2015/06/code/clustering-markers-on-leaflet-maps/
         * http://leafletjs.com/2012/08/20/guest-post-markerclusterer-0-1-released.html
         *
         * @type {{map: {enable: string[], logic: string}, marker: {enable: Array, logic: string}}}
         */
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

        /**
         * Initialization for leaflet.js
         *
         * DEPRECATED (for reference only)
         * REPLACED BY ui-leaflet
         */
        // // initialize the map
        // var map = L.map('map').setView([51.33843, 12.37866], 17);
        //
        // // load a tile layer
        // L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
        // attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
        // }).addTo(map);
        //
        // // add markers
        // L.marker([51.33843, 12.37866]).addTo(map)
        // .bindPopup('@user: Universität Leipzig! <3<br>' +
        // '[51.33843, 12.37866]<br>' +
        // 'Tweet metadata here!')
        // .openPopup();
        //
        // L.marker([51.33948, 12.37637]).addTo(map)
        // .bindPopup('@user: MGM-TP<br>' +
        // '[51.33948, 12.37637]<br>' +
        // 'Tweet metadata here!')
    }
})();
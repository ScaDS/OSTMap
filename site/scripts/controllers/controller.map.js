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

    function MapCtrl($scope, httpService, $log, nemSimpleLogger, leafletData) {
        mapInit($scope);

        $scope.currentFilters = "";
        $scope.timeFilter = '1h';
        $scope.search = [];
        $scope.search.hashtagFilter = "#";
        $scope.search.searchFilter = "[PLACEHOLDER: Search Filter]";

        $scope.search.clearFilters = function () {
            $scope.search.timeFilter = "None";
            $scope.search.hashtagFilter = "#";
            $scope.center ={
                lat: 50,
                lng: 12,
                zoom: 4
            }

            $scope.search.updateFilters();
        }
        $scope.search.setHashtagFilter = function (hashtag) {
            $scope.search.hashtagFilter = "#" + hashtag;
            $scope.search.updateFilters();
        }
        $scope.search.updateFilters = function () {
            httpService.getTweetsFromLocal();
            // $scope.populateMarkers();
            $scope.getBounds();

            $scope.currentFilters = $scope.timeFilter + " | " +
                $scope.search.hashtagFilter + " | " +
                $scope.search.searchFilter + " | " +
                $scope.center.lat + "/" + $scope.center.lng + "/" + $scope.center.zoom;
        }

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

        $scope.populateMarkers = function () {
            console.log("Test Object:")
                var test = httpService.getTweets();
                console.log(test[1])

            var tweet;
            console.log("Tweets:")
            for(tweet in $scope.data.tweets) {
                // console.dir(tweet)
                console.dir(tweet.coordinates.coordinates)

                if(typeof tweet.coordinates != 'undefined') {
                    console.log("Coords = ")
                    console.log(tweet.geo.coordinates)
                    console.log(tweet.coordinates.coordinates)

                    $scope.markers.add(tweet.coordinates.coordinates)
                }
            }
        }

        $scope.currentBounds = null;
        $scope.getBounds = function () {
            leafletData.getMap().then(
                function(map) {
                    $scope.currentBounds = map.getBounds();
                    console.dir($scope.currentBounds);
                }
            );
        }

        $scope.search.updateFilters();
    }

    function mapInit($scope) {
        $scope.center ={
            lat: 50,
            lng: 12,
            zoom: 4
        }
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
        }
        $scope.maxBounds = $scope.regions.europe

        $scope.markers = [
            {
                lat: 51.33843,
                lng: 12.37866,
                focus: true,
                draggable: false,
                message: "Test Marker",
                icon: {}
            }
        ];

        $scope.events = {
            map: {
                enable: ['moveend', 'popupopen'],
                    logic: 'emit'
            },
            marker: {
                enable: [],
                    logic: 'emit'
            }
        }


        /*
         //Leaflet Init
         // initialize the map
         var map = L.map('map').setView([51.33843, 12.37866], 17);

         // load a tile layer
         L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
         attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
         }).addTo(map);

         // add markers
         L.marker([51.33843, 12.37866]).addTo(map)
         .bindPopup('@user: Universität Leipzig! <3<br>' +
         '[51.33843, 12.37866]<br>' +
         'Tweet metadata here!')
         .openPopup();

         L.marker([51.33948, 12.37637]).addTo(map)
         .bindPopup('@user: MGM-TP<br>' +
         '[51.33948, 12.37637]<br>' +
         'Tweet metadata here!')
         */
    }
})();
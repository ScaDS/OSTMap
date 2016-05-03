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

    function MapCtrl($scope, httpService, $log, leafletData) {
        $scope.currentFilters = "";
        $scope.timeFilter = '1h';
        $scope.search = [];
        $scope.search.hashtagFilter = "#";
        $scope.search.searchFilter = "[PLACEHOLDER: Search Filter]";

        $scope.search.clearFilter = function () {
            $scope.search.timeFilter = "None";
            $scope.search.hashtagFilter = "#";
            $scope.search.updateFilters();
        }
        $scope.search.setHashtagFilter = function (hashtag) {
            $scope.search.hashtagFilter = "#" + hashtag;
            $scope.search.updateFilters();
        }
        $scope.search.updateFilters = function () {
            httpService.getTweetsFromLocal();

            $scope.currentFilters = $scope.timeFilter + " | " +
                $scope.search.hashtagFilter + " | " +
                $scope.search.searchFilter;
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


        $scope.search.updateFilters();
        mapInit($scope);

        $scope.currentBounds = null;
        console.log("leafletData ");
        console.log(leafletData);
        $scope.getBounds = function () {
            leafletData.getMap().then(
                function(map) {
                    $scope.currentBounds = map.getBounds();

                    console.log("map.getBounds()");
                    console.log(map.getBounds());
                }
            );
        }
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
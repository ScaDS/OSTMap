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
        'httpService'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @param $location
     * @param httpService
     * @constructor
     */
    function ListCtrl($scope, $location, httpService) {
        mapInit($scope);
        
        $scope.search = [];
        $scope.data = [];

        $scope.search.inputValue = httpService.getSearchToken();
        $scope.search.searchFields = httpService.getSearchFields();
        console.log();

        $scope.dataSource = "localhost";

        /**
         * Get the tweets array from the httpService
         */
        $scope.data.tweets = httpService.getTweets();

        var updateQueued = false;
        $scope.search.updateFilters = function (mode) {
            if (!httpService.getLoading()) {
                httpService.setLoading(true);

                httpService.setSearchToken($scope.search.inputValue);
                httpService.setSearchFields($scope.search.searchFields);

                /**
                 * get the tweets from the REST interface
                 */
                if ($scope.dataSource == "accumulo") {
                    httpService.getTweetsFromServerByToken()
                } else if ($scope.dataSource == "localhost") {
                    httpService.getTweetsFromServerByToken2()
                } else if ($scope.dataSource == "static") {
                    httpService.getTweetsFromLocal()
                }

                if (mode && mode === 'list') {

                } else if (mode && mode === 'map') {
                    //TODO: Call Service to load Data for the Map view
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
            console.log("Chunk loading: " + processed + "/" + total + " " + elapsed + "ms")
        }
    }
})();
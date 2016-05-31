/**
 * The controller for the analytics view.
 */
(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .controller('RankingsCtrl', RankingsCtrl);

    /**
     * Inject all dependencies for the controller
     * $scope to interact with the view
     * @type {string[]}
     */
    RankingsCtrl.$inject = [
        '$scope',
        'httpService',
        '$log',
        'nemSimpleLogger',
        'leafletData'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @constructor
     */
    function RankingsCtrl($scope, httpService, $log, nemSimpleLogger, leafletData) {
        mapInit($scope);

        $scope.autoUpdate = false;
        $scope.dataSource = "localhost"; //default: "accumulo";
        $scope.clusteringEnabled = true;
        $scope.usePruneCluster = true;

        $scope.data = httpService.getHighScore();
        $scope.data.paths = {};

        $scope.goToHighScoreArea = function(data) {
            $scope.data.paths = {};

            var newPath = {
                p1: {
                    color: 'red',
                    weight: 8,
                    latlngs: [],
                    // message: '<h3>User path: ' + data.user + '</h3><p>Distance: ' + data.distance + ' km</p>'
                }
            };

            data.coordinates.forEach(function(point) {
                newPath.p1.latlngs.push(
                    {
                        lat: point[0],
                        lng: point[1]
                    }
                )
            });

            newPath.p1.latlngs.push(
                {
                    lat: data.coordinates[0][0],
                    lng: data.coordinates[0][1]
                }
            )


            $scope.data.paths = newPath;
        };

        $scope.goToHighScoreDistance = function(data) {
            $scope.data.paths = {};

            var newPath = {
                p1: {
                    color: 'red',
                    weight: 8,
                    latlngs: [],
                    // message: '<h3>User path: ' + data.user + '</h3><p>Distance: ' + data.distance + ' km</p>'
                }
            };

            data.coordinates.forEach(function(point) {
                newPath.p1.latlngs.push(
                    {
                        lat: point[0],
                        lng: point[1]
                    }
                )
            });


            $scope.data.paths = newPath;
        };

        /**
         * Populate the maps
         */
        $scope.populateMaps = function () {
            /**
             * Iterate through data
             */
            // $scope.data.forEach(function(tweet) {
            //
            // });
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
            /**
             * If data was already fetched previously, load it
             */
            if ($scope.data.path.length > 0 && $scope.data.area.length > 0) {
                console.log("Existing Data Path: " + $scope.data.path.length);
                console.log("Existing Data Area: " + $scope.data.area.length);
                $scope.populateMaps();
            } else {
                /**
                 * get the tweets from the REST interface
                 */
                if ($scope.dataSource == "accumulo") {
                    httpService.getHighScoreFromServer().then(function () {
                        $scope.populateMaps();
                    });
                } else if ($scope.dataSource == "localhost") {
                    httpService.getHighScoreFromServer().then(function () {
                        $scope.populateMaps();
                    });
                } else if ($scope.dataSource == "static") {
                    $scope.data = {"area":[{"user":"Zorne","area":6174893.727618582,"coordinates":[[-0.2147884,-0.4614716],[27.2147884,38.4614716],[49,26.546974]]},{"user":"Falk","area":2465661.8600413124,"coordinates":[[0,0],[10,10],[10,20],[20,0]]},{"user":"Oliver","area":54980.275141825,"coordinates":[[-2.2147884,52.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-8.2147884,53.4614716]]}],"path":[{"user":"Zorne","distance":7589.900654023497,"coordinates":[[49,26.546974],[27.2147884,38.4614716],[-0.2147884,-0.4614716]]},{"user":"Falk","distance":5818.530021620169,"coordinates":[[0,0],[20,0],[10,20],[10,10],[10,10],[10,10],[10,10]]},{"user":"Peter","distance":4925.676750216901,"coordinates":[[-3.2147884,53.4614716],[47.2147884,28.4614716]]},{"user":"Oliver","distance":1062.1904270002783,"coordinates":[[-3.2147884,53.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-2.2147884,52.4614716],[-8.2147884,53.4614716]]}]};
                    $scope.populateMaps();
                }
            }
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
            zoom: 2
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
            // overlays: {
            //     cluster: {
            //         name: "Clustered Markers",
            //         type: "markercluster",
            //         visible: true,
            //         layerOptions: {
            //             "chunkedLoading": true,
            //             "showCoverageOnHover": false,
            //             "removeOutsideVisibleBounds": true,
            //             "chunkProgress": updateProgressBar
            //         }
            //     },
            //     dots: {
            //         name: "Red Dots",
            //         type: "group",
            //         visible: true,
            //         layerOptions: {
            //             "chunkedLoading": true,
            //             "showCoverageOnHover": false,
            //             "removeOutsideVisibleBounds": true,
            //             "chunkProgress": updateProgressBar
            //         }
            //     }
            // }
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
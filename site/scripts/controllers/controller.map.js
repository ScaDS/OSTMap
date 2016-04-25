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
        '$scope'
    ];

    function MapCtrl($scope) {
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

    }
})();
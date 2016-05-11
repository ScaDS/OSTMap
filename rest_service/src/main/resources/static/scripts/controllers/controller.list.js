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
    function ListCtrl($scope,$location,httpService) {
        $scope.search = [];
        $scope.data = [];

        $scope.search.inputValue = httpService.getSearchToken();
        $scope.search.searchFields = httpService.getSearchFields();
        console.log();

        $scope.dataSource = "accumulo";
        document.getElementById("loading").style.visibility = "hidden";

        /**
         * Get the tweets array from the httpService
         */
        $scope.data.tweets = httpService.getTweets();

        $scope.search.onClick = function (mode) {

            httpService.setSearchToken($scope.search.inputValue);
            httpService.setSearchFields($scope.search.searchFields);


            if ($scope.dataSource == "accumulo") {
                httpService.getTweetsFromServerByToken();    //Get by Token
            } else if ($scope.dataSource == "restTest") {
                httpService.getTweetsFromServerTest();       //Get using test REST API
            } else if ($scope.dataSource == "static") {
                httpService.getTweetsFromLocal();            //Get from local (debug)
            } else {
                httpService.getTweetsFromServerByToken();    //Get by Token
            }
            // httpService.getTweetsFromServerByToken();

            if (mode && mode === 'list') {

            } else if (mode && mode === 'map') {
                //TODO: Call Service to load Data for the Map view
            }
        }
    }
})();
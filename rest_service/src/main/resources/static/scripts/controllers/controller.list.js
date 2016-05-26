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
                } else if ($scope.dataSource == "restTest") {
                    httpService.getTweetsFromServerTest()
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
    }
})();
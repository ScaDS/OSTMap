/**
 * Created by Christopher on 24.04.2016.
 */

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
        'httpService'
    ];

    /**
     * The controller logic
     *
     * @param $scope
     * @param httpService
     * @constructor
     */
    function ListCtrl($scope,httpService) {
        $scope.search = [];
        $scope.data = [];

        /**
         * Get the tweets array from the httpService
         */
        $scope.data.tweets = httpService.getTweets();

        /**
         * Get all checkboxes to set the search area
         */
        $scope.search.checkBoxes = getSearchFields();


        $scope.search.onClick = function (mode) {
            /**
             * Get the tweets from the json file
             */
            httpService.getTweetsFromLocal();

            if (mode && mode === 'list') {

            } else if (mode === 'map') {

            }
        }
    }

    /**
     * A definition of all checkboxes
     * @returns {*[]}
     */
    function getSearchFields() {
        return [
            {
                fieldname: 'text',
                checked: true
            },
            {
                fieldname: 'hashtag',
                checked: true
            },
            {
                fieldname: 'user',
                checked: false
            },
            {
                fieldname: 'location',
                checked: false
            }
        ]
    }
})();
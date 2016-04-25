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

    ListCtrl.$inject = [
        '$scope',
        'httpService'
    ];

    function ListCtrl($scope,httpService) {
        $scope.search = [];
        $scope.data = [];

        $scope.data.tweets = httpService.getTweets();

        $scope.search.checkBoxes = getSearchFields();
        $scope.search.onClick = function (mode) {
            httpService.getTweetsFromLocal();

            if (mode && mode === 'list') {

            } else if (mode === 'map') {

            }
        }
    }



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
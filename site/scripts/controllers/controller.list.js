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
        '$scope'
    ];

    function ListCtrl($scope) {
        $scope.search = [];
        $scope.search.checkBoxes = getSearchFields();
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
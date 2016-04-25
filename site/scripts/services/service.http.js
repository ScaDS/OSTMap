/**
 * Created by Christopher on 25.04.2016.
 */

(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .service('httpService', httpService);

    httpService.$inject = [
        '$http'
    ];

    var _tweets = [];
    var _serverUrl = "http://...";

    function httpService($http) {
        return {
            getTweetsFromServer: _getTweetsFromServer,
            getTweetsFromLocal: _getTweetsFromLocal,
            getTweets: _getTweets
        };

        function _getTweetsFromServer() {
            var url = _serverUrl + "something";
            $http.get(url).success(function (data, status, headers, config) {
                //Save the result
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
            });
        }

        function _getTweetsFromLocal() {
            var url = "data/example-response.json";
            $http.get(url).then(function (result) {
                if(result.status == 200){
                   angular.copy(result.data,_tweets);
                }
            });
        }

        function _getTweets() {
            return _tweets;
        }

    }

})();
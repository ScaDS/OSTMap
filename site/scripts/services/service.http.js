/**
 * Created by Christopher on 25.04.2016.
 */

(function () {
    'use strict';

    angular
        .module('ostMapApp')
        .service('httpService', httpService);

    /**
     * Inject dependencies for the service
     * $http to load content from remote url
     * @type {string[]}
     */
    httpService.$inject = [
        '$http'
    ];

    /**
     * Array to store all tweets
     * @type {Array}
     * @private
     */
    var _tweets = [];

    /**
     * The URI of the webservice
     * @type {string}
     * @private
     */
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

        /**
         * Reads tweets from the local example json
         * @private
         */
        function _getTweetsFromLocal() {
            var url = "data/example-response.json";
            $http.get(url).then(function (result) {
                if(result.status == 200){
                    //Copy result data to the private array
                   angular.copy(result.data,_tweets);
                }
            });
        }

        /**
         * Getter method for _tweets to access from outside
         * @returns {Array}
         * @private
         */
        function _getTweets() {
            return _tweets;
        }

    }

})();
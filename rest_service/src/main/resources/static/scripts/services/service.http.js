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
     * The bounding box to search in
     * @param bbnorth the northern latitude for the bounding box to search, e.g. 10.123
     * @param bbwest the western longitude for the bounding box to search, e.g. 30.123
     * @param bbsouth the southern latitude for the bounding box to search, e.g. -10.456
     * @param bbeast the eastern longitude for the bounding box to search, e.g. -30.789
     * @type {{bbnorth: number, bbwest: number, bbsouth: number, bbeast: number}}
     * @private
     */
    var _boundingBox = {
        bbnorth: 0.0,
        bbwest: 0.0,
        bbsouth: 0.0,
        bbeast: 0.0
    };

    /**
     * The time period to search in (s unix time, e.g. 1461942000000)
     * @type {{tstart: string, tend: string}}
     * @private
     */
    var _timePeriod = {
        tstart: "0000000000",
        tend: "0000000000"
    };

    /**
     * The possible search fields as array
     * @type {*[]}
     * @private
     */
    var _searchFields =
    {
        text: {
            checked: true
        },
        user: {
            checked: false
        }
    };

    /**
     * The token to search for, e.g. #yolo or yolo
     * @type {string}
     * @private
     */
    var _searchToken = "";

    function httpService($http) {
        return {
            getTweetsFromServerByToken: _getTweetsFromServerByToken,
            getTweetsFromServerByGeoTime: _getTweetsFromServerByGeoTime,
            getTweetsFromLocal: _getTweetsFromLocal,
            getTweets: _getTweets,
            getSearchToken: _getSearchToken,
            setSearchToken: _setSearchToken,
            getSearchFields: _getSearchFields,
            setSearchFields: _setSearchFields,
            getBoundingBox: _getBoundingBox,
            setBoundingBox: _setBoundingBox,
            getTimeWindow: _getTimeWindow,
            setTimeWindow: _setTimeWindow,
        };

        function _getTweetsFromServerByToken() {
            var url = getTokenSearchUrl();
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                angular.copy(data,_tweets);
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
            });
        }

        function _getTweetsFromServerByGeoTime() {
            var url = getGeoTemporalSearchUrl();
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                angular.copy(data,_tweets);
                console.log("HTTP response received")
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

        /**
         * Getter for _searchToken
         * @returns {string}
         * @private
         */
        function _getSearchToken() {
            return _searchToken;
        }

        /**
         * Setter for _searchToken
         * @param token
         * @private
         */
        function _setSearchToken(token) {
            _searchToken = token;
        }

        /**
         * Getter for _searchFields
         * @returns {*[]}
         * @private
         */
        function _getSearchFields(){
            return _searchFields;
        }

        /**
         * Setter for _searchFields
         * @param searchFields
         * @private
         */
        function _setSearchFields(searchFields){
            _searchFields = searchFields;
        }

        /**
         * Builds the webservice url for token search
         * @returns {string} the request url with all query params
         */
        function getTokenSearchUrl()
        {
            return "/api/tokensearch?field=" + buildFieldString() + "&token=" + _searchToken;
        }

        /**
         * Builds the webservice url for tweet search in a bounding box and a time period
         * @returns {string} the request url with all query params
         */
        function getGeoTemporalSearchUrl()
        {
            return "/api/geotemporalsearch?bbnorth=" + _boundingBox.bbnorth
            // return "http://localhost:8080/api/geotemporalsearch?bbnorth=" + _boundingBox.bbnorth  //for debugging
                + "&bbsouth=" +  _boundingBox.bbsouth
                + "&bbeast=" +  _boundingBox.bbeast
                + "&bbwest=" +  _boundingBox.bbwest
                + "&tstart=" + _timePeriod.tstart
                + "&tend=" + _timePeriod.tend;
        }

        /**
         * Builds a comma separated list of the search fields.
         * @returns {string}
         */
        function buildFieldString()
        {
            var checkedFields = [];
            angular.forEach(_searchFields, function(value, key) {
                if(value.checked){
                    this.push(key);
                }
            },checkedFields);
            return checkedFields.join(',');
        }

        /**
         * Getter for _boundingBox
         * @returns {{bbnorth: number, bbwest: number, bbsouth: number, bbeast: number}}
         * @private
         */
        function _getBoundingBox(){
            return _boundingBox;
        }

        /**
         * Setter for _boundingBox
         * @param north
         * @param west
         * @param south
         * @param east
         * @private
         */
        // function _setBoundingBox(north, west, south, east){
        //     _boundingBox = {
        //         bbnorth: north,
        //         bbwest: west,
        //         bbsouth: south,
        //         bbeast: east
        //     };
        // }
        function _setBoundingBox(bounds){
            _boundingBox = bounds
        }

        /**
         * Getter for _timePeriod
         * @returns {{tstart: string, tend: string}}
         * @private
         */
        function _getTimeWindow(){
            return _timePeriod;
        }

        /**
         * Setter for _timePeriod
         * @param start
         * @param end
         * @private
         */
        function _setTimeWindow(times){
            _timePeriod = {
                tstart: times[0],
                tend: times[0]
            };
        }
    }
})();
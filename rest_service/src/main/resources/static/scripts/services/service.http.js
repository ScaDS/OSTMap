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
        '$rootScope',
        '$http',
        '$q'
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
     * @type {{text: {checked: boolean}, user: {checked: boolean}}}
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

    /**
     * Loading status
     * @type {boolean}
     */
    var isLoading = false;

    var maxQueueSize = 2;

    var maxSimultaneous = 1;

    var queue = [];

    var processing = [];

    function httpService($rootScope, $http, $q) {
        return {
            getTweetsFromServerByToken: _getTweetsFromServerByToken,
            getTweetsFromServerByGeoTime: _getTweetsFromServerByGeoTime,
            getTweetsFromServerTest: _getTweetsFromServerTest,
            getTweetsFromServerTest2: _getTweetsFromServerTest2,
            getTweetsFromLocal: _getTweetsFromLocal,
            queueAddGetTweetFrom: _queueAddGetTweetFrom,
            removeFromQueue: _removeFromQueue,
            getTweets: _getTweets,
            getSearchToken: _getSearchToken,
            setSearchToken: _setSearchToken,
            getSearchFields: _getSearchFields,
            setSearchFields: _setSearchFields,
            getBoundingBox: _getBoundingBox,
            setBoundingBox: _setBoundingBox,
            getTimeWindow: _getTimeWindow,
            setTimeWindow: _setTimeWindow,
            getLoading: _getLoading,
            setLoading: _setLoading
        };

        function _queueAddGetTweetFrom(api, filters) {
            if (queue.size < maxQueueSize) {
                
            }

            switch(api) {
                case "accumulo":
                    $rootScope.$emit('alertControl', api);
                    break;
                case "restTest":
                    $rootScope.$emit('alertControl', api);
                    break;
                case "static":
                    $rootScope.$emit('alertControl', api);
                    break;
                default:
                    $rootScope.$emit('alertControl', api);
            }

            processQueue();
        }

        function _removeFromQueue(index) {
            queue.splice(index, 1);
        }

        function processQueue() {
            if (processing.size < maxSimultaneous) {
                processQueue();
            } else {

            }
        }

        function _getTweetsFromServerByToken() {
            _setLoading(true);
            var deferred = $q.defer();

            var url = getTokenSearchUrl();
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                //angular.copy(data,_tweets);
                _tweets = _.clone(data);
                _setLoading(status);
                deferred.resolve(status);
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
                _setLoading(status);
                deferred.resolve(status + "\n" + headers + "\n" + config);
            });

            return deferred.promise;
        }

        function _getTweetsFromServerByGeoTime() {
            _setLoading(true);
            var deferred = $q.defer();

            var url = getGeoTemporalSearchUrl();
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                // angular.copy(data,_tweets);
                _tweets = _.clone(data);
                _setLoading(status);
                deferred.resolve(status);
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
                _setLoading(status);
                deferred.resolve(status + "\n" + headers + "\n" + config);
            });

            return deferred.promise;
        }
        function _getTweetsFromServerTest() {
            _setLoading(true);
            var deferred = $q.defer();

            var url = "http://localhost:8080/api/testgeo"
                + "?bbnorth=" + _boundingBox.bbnorth
                + "&bbsouth=" +  _boundingBox.bbsouth
                + "&bbeast=" +  _boundingBox.bbeast
                + "&bbwest=" +  _boundingBox.bbwest
                + "&tstart=" + _timePeriod.tstart
                + "&tend=" + _timePeriod.tend;
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                // angular.copy(data,_tweets);
                _tweets = _.clone(data);
                _setLoading(status);
                deferred.resolve(status);
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
                _setLoading(status);
                deferred.resolve(status + "\n" + headers + "\n" + config);
            });

            return deferred.promise;
        }
        function _getTweetsFromServerTest2() {
            _setLoading(true);
            var deferred = $q.defer();

            var url = "http://localhost:8082/api/geotemporalsearch"
                + "?bbnorth=" + _boundingBox.bbnorth
                + "&bbsouth=" +  _boundingBox.bbsouth
                + "&bbeast=" +  _boundingBox.bbeast
                + "&bbwest=" +  _boundingBox.bbwest
                + "&tstart=" + _timePeriod.tstart
                + "&tend=" + _timePeriod.tend;
            $http.get(url).success(function (data, status, headers, config) {
                //Copy result data to the private array
                // angular.copy(data,_tweets);
                _tweets = _.clone(data);
                _setLoading(status);
                deferred.resolve(status);
            }).error(function (data, status, headers, config) {
                //TODO: Log the errors
                _setLoading(status);
                deferred.resolve(status + "\n" + headers + "\n" + config);
            });

            return deferred.promise;
        }

        /**
         * Reads tweets from the local example json
         * @private
         */
        function _getTweetsFromLocal() {
            _setLoading(true);
            var deferred = $q.defer();

            // var url = "data/small-response.json";
            var url = "data/large-response.json";
            $http.get(url).then(function (data) {
                setTimeout(function(){
                    if(data.status == 200){
                        //Copy result data to the private array

                        // angular.copy(data.data,_tweets); //1595ms very slow
                        _tweets = _.clone(data.data);
                        // _tweets = result.data;

                        _setLoading(status);
                        deferred.resolve(data.status);
                    } else {
                        _setLoading(status);
                        deferred.resolve(data.status + "\n" + data.headers + "\n" + data.config);
                    }
                }, 1);

            });

            return deferred.promise;
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
            var urlEncodedSearchToken = encodeURI(_searchToken);
            //replace # with %23
            urlEncodedSearchToken = urlEncodedSearchToken.replace(/#/g, '%23');
            return "/api/tokensearch?field=" + encodeURI(buildFieldString()) + "&token=" + urlEncodedSearchToken;
        }

        /**
         * Builds the webservice url for tweet search in a bounding box and a time period
         * @returns {string} the request url with all query params
         */
        function getGeoTemporalSearchUrl()
        {
            return "/api/geotemporalsearch?bbnorth=" + _boundingBox.bbnorth
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
         * @private
         * @param bounds
         */
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
         * @private
         * @param times
         */
        function _setTimeWindow(times){
            _timePeriod = {
                tstart: times[0],
                tend: times[1]
            };
        }

        function _getLoading(){
            return isLoading;
        }

        function _setLoading(status){
            if (!status || status == 200) {
                isLoading = false;
            } else if (status) {
                isLoading = true;
            } else {
                isLoading = false;
            }
        }
    }
})();
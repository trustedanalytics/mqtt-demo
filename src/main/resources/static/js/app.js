/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

var App = angular.module('app', []);

(function(){

    App.controller('MainController', ['$scope', 'ChartFactory', '$http', '$timeout', '$location', function($scope, ChartFactory, $http, $timeout, $location){
        var dataProvider = [];

        $scope.since = $location.search().since || "1d";

        var chart = ChartFactory.create("chartdiv", dataProvider, [
            {
                valueField: '1.0',
                title: 'Rad Flow'
            }, {
                valueField: '2.0',
                title: 'Fpv Close'
            }, {
                valueField: '3.0',
                title: 'Fpv Open'
            }, {
                valueField: '4.0',
                title: 'High'
            }, {
                valueField: '5.0',
                title: 'Bypass'
            }, {
                valueField: '6.0',
                title: 'Bpv Close'
            }, {
                valueField: '7.0',
                title: 'Bpv Open'
            }
        ]);

        $scope.$watch('since', function(){
            refreshData($http, $timeout, $scope, chart);
        });

        refreshData($http, $timeout, $scope, chart);
    }]);

    function refreshData($http, $timeout, $scope, chart) {
        $http.get('/rest/space-shuttle/chart?since=' + $scope.since)
            .then(function onSuccess(data){
                var series = _.sortBy(_.pairs(data.data).map(function(d){
                    d[1].timestamp = d[0];
                    return d[1];
                }), 'timestamp');
                chart.dataProvider = series;
                chart.validateData();
            })
            .finally(function onFinally(){
                $timeout(function () {
                    refreshData($http, $timeout, $scope, chart);
                }, 5000);
            });
    }
})();

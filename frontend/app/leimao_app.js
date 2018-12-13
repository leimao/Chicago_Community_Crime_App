const express = require('express');
const bodyParser = require('body-parser');
const hbase = require('hbase-rpc-client');
var async = require("async");
//var Int64 = require('node-int64');
const app = express();
const hostname = '127.0.0.1';
const port = 3000;
const hbaseTableCrimeByWeather = 'leimao_community_crime_by_weather';
const hbaseTableTaxiTrips = 'leimao_taxi_trips_by_community_chicago';
const kafkaTopicCrime = 'leimao_crime_update';
const kafkaTopicWeather = 'leimao_weather_update';

//app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.set('views', __dirname + '/views');
app.use(express.static(__dirname + '/public'));
app.set('view engine', 'ejs');

var client = hbase({
    /* Zookeeper information for VM */
    zookeeperHosts: ['localhost:2181'],
    zookeeperRoot: '/hbase'
    /* Zookeeper information for cluster */
    //zookeeperHosts: ['10.0.0.2:2181'],
    //zookeeperRoot: '/hbase-unsecure'
});

client.on('error', function(err) {
    console.log(err);
});

app.get('/', function (req, res) {
    // res.send('Hellow Underworld!');
    res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: null});
});


app.post('/', function (req, res) {
    // Chicago Community Information
    // https://en.wikipedia.org/wiki/Community_areas_in_Chicago
    // const community = 'hyde park';
    const community = req.body.community.trim().toLowerCase();
    const key = new hbase.Get(community);

    if (community == "-") {
        res.render('index', {crime_weather: null, error_weather: "Please select a valid Chicago community!", crime_taxi: null, error_taxi: null});
        return;
    }

    client.get(hbaseTableCrimeByWeather, key, function(err, row) {

        if (err) {
            res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'Some error just happened.'});
            return;
        } 

        if (!row) {
            //res.send('<html><body>No such community in Chicago!</body></html>');
            res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'No such community in Chicago or no crime found.'});
            return;
        }

        function crimeAvg(weather) {
            var numCrimesBuffer = row.cols[`crime:${weather}_crime_sum`].value;
            var numCrimesBigInt = BigInt(`0x${numCrimesBuffer.toString('hex', 0, 8)}`);
            var numCrimes = Number(numCrimesBigInt);
            //console.log(numCrimes)
            var numDaysBuffer = row.cols[`crime:${weather}_days`].value;
            var numDaysBigInt = BigInt(`0x${numDaysBuffer.toString('hex', 0, 8)}`);
            var numDays = Number(numDaysBigInt);

            //var areaBuffer = row.cols['crime:communityArea'].value;
            //var areaBigInt = BigInt(`0x${areaBuffer.toString('hex', 0, 8)}`);
            //var area = Number(areaBigInt);
            var area = row.cols['crime:communityArea'].value;

            /* Alternative solution */
            // https://github.com/nodejs/node/issues/21956#issuecomment-445477288
            // var int64 = new Int64(numCrimesBuffer);
            // var int32 = int64.toNumber(true)

            const scaleFactor = 1e8;
            if (numDays == 0) {
                return 'NULL';
            }
            else {
                return (numCrimes/(numDays*area)*scaleFactor).toFixed(1); // One decimal place
            }
        }

        //var communityName = row.cols['crime:communityId'].value;
        var communityName = community.toLowerCase().split(' ').map((s) => s.charAt(0).toUpperCase() + s.substring(1)).join(' ');
        var crimeFog = crimeAvg('fog');
        var crimeRain = crimeAvg('rain');
        var crimeSnow = crimeAvg('snow');
        var crimeHail = crimeAvg('hail');
        var crimeThunder = crimeAvg('thunder');
        var crimeTornado = crimeAvg('tornado');
        var crimeClear = crimeAvg('clear');

        //res.send(`${communityName}, ${crimeFog}, ${crimeRain}, ${crimeSnow}, ${crimeHail}, ${crimeThunder}, ${crimeTornado}, ${crimeClear}`);
        res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: true, error_weather: null, communityName: communityName, crimeClear: crimeClear, crimeFog: crimeFog, crimeRain: crimeRain, crimeSnow: crimeSnow, crimeHail: crimeHail, crimeThunder: crimeThunder, crimeTornado: crimeTornado});
    })

});


app.post('/taxi', function (req, res) {

    const community_pickup = req.body.community_pickup.trim().toLowerCase();
    const community_dropoff = req.body.community_dropoff.trim().toLowerCase();
    const community_pickup_key = new hbase.Get(community_pickup);
    const community_dropoff_key = new hbase.Get(community_dropoff);
    const routeGo_key = new hbase.Get(community_pickup + '-' + community_dropoff);
    const routeBack_key = new hbase.Get(community_dropoff + '-' + community_pickup);

    if ((community_pickup == "-") || (community_dropoff == "-")) {
        res.render('index', {crime_weather: null, error_weather: null, crime_taxi: null, error_taxi: "Please select valid Chicago communities!"});
        return;
    }

    var obj = {"pickup": [community_pickup_key, hbaseTableCrimeByWeather], "dropoff": [community_dropoff_key, hbaseTableCrimeByWeather], "go": [routeGo_key, hbaseTableTaxiTrips], "back": [routeBack_key, hbaseTableTaxiTrips]};
    var configs = {};

    client.mget(hbaseTableCrimeByWeather, [community_pickup_key, community_dropoff_key], function(err, row) {

        if (err) {
            res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'Some error just happened.'});
            return;
        } 
        if (!row) {
            res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'No such community in Chicago or no crime found.'});
            return;
        }

        community_pickup_row = row[0];
        community_dropoff_row = row[1];

        function crimeAvg(row, weather) {
            var numCrimesBuffer = row.cols[`crime:${weather}_crime_sum`].value;
            var numCrimesBigInt = BigInt(`0x${numCrimesBuffer.toString('hex', 0, 8)}`);
            var numCrimes = Number(numCrimesBigInt);

            var numDaysBuffer = row.cols[`crime:${weather}_days`].value;
            var numDaysBigInt = BigInt(`0x${numDaysBuffer.toString('hex', 0, 8)}`);
            var numDays = Number(numDaysBigInt);

            var area = row.cols['crime:communityArea'].value;

            const scaleFactor = 1e8;
            if (numDays == 0) {
                return 'NULL';
            }
            else {
                return (numCrimes/(numDays*area)*scaleFactor).toFixed(1); // One decimal place
            }
        }

        var communityName1 = community_pickup.toLowerCase().split(' ').map((s) => s.charAt(0).toUpperCase() + s.substring(1)).join(' ');
        var crimeFog1 = crimeAvg(community_pickup_row, 'fog');
        var crimeRain1 = crimeAvg(community_pickup_row, 'rain');
        var crimeSnow1 = crimeAvg(community_pickup_row, 'snow');
        var crimeHail1 = crimeAvg(community_pickup_row, 'hail');
        var crimeThunder1 = crimeAvg(community_pickup_row, 'thunder');
        var crimeTornado1 = crimeAvg(community_pickup_row, 'tornado');
        var crimeClear1 = crimeAvg(community_pickup_row, 'clear');


        var communityName2 = community_dropoff.toLowerCase().split(' ').map((s) => s.charAt(0).toUpperCase() + s.substring(1)).join(' ');
        var crimeFog2 = crimeAvg(community_dropoff_row, 'fog');
        var crimeRain2 = crimeAvg(community_dropoff_row, 'rain');
        var crimeSnow2 = crimeAvg(community_dropoff_row, 'snow');
        var crimeHail2 = crimeAvg(community_dropoff_row, 'hail');
        var crimeThunder2 = crimeAvg(community_dropoff_row, 'thunder');
        var crimeTornado2 = crimeAvg(community_dropoff_row, 'tornado');
        var crimeClear2 = crimeAvg(community_dropoff_row, 'clear');

        client.mget(hbaseTableTaxiTrips, [routeGo_key, routeBack_key], function(err, row) {

            if (err) {
                res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'Some error just happened.'});
                return;
            } 
            if (!row) {
                res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'No such community in Chicago or no crime found.'});
                return;
            }

            route_go_row = row[0];
            route_back_row = row[1];

            var date_today = new Date();
            date_today.setHours(0,0,0,0);
            var date_first = new Date("2013-01-01");
            var date_diff = Math.floor((date_today-date_first) / (1000 * 3600 * 24));

            var routeDurationSumBuffer = route_go_row.cols['taxi:trip_duration_sum'].value;
            var routeDurationSumBigInt = BigInt(`0x${routeDurationSumBuffer.toString('hex', 0, 8)}`);
            var routeDurationSum = Number(routeDurationSumBigInt);
            var totalCostSumBuffer = route_go_row.cols['taxi:total_cost_sum'].value;
            var totalCostSumBigInt = BigInt(`0x${totalCostSumBuffer.toString('hex', 0, 8)}`);
            var totalCostSum = Number(totalCostSumBigInt);
            var numTripsBuffer = route_go_row.cols['taxi:num_trips'].value;
            var numTripsBigInt = BigInt(`0x${numTripsBuffer.toString('hex', 0, 8)}`);
            var numTrips = Number(numTripsBigInt);
            var costAvg1 = (totalCostSum / numTrips).toFixed(1);
            var timeAvg1 = (routeDurationSum / numTrips / 60).toFixed(1);
            var frequency1 = (numTrips / date_diff).toFixed(1);

            var routeDurationSumBuffer = route_back_row.cols['taxi:trip_duration_sum'].value;
            var routeDurationSumBigInt = BigInt(`0x${routeDurationSumBuffer.toString('hex', 0, 8)}`);
            var routeDurationSum = Number(routeDurationSumBigInt);
            var totalCostSumBuffer = route_back_row.cols['taxi:total_cost_sum'].value;
            var totalCostSumBigInt = BigInt(`0x${totalCostSumBuffer.toString('hex', 0, 8)}`);
            var totalCostSum = Number(totalCostSumBigInt);
            var numTripsBuffer = route_back_row.cols['taxi:num_trips'].value;
            var numTripsBigInt = BigInt(`0x${numTripsBuffer.toString('hex', 0, 8)}`);
            var numTrips = Number(numTripsBigInt);
            var costAvg2 = (totalCostSum / numTrips).toFixed(1);
            var timeAvg2 = (routeDurationSum / numTrips / 60).toFixed(1);
            var frequency2 = (numTrips / date_diff).toFixed(1);

            res.render('index', {crime_weather: null, error_weather: null, crime_taxi: true, error_taxi: null, 
                communityName1: communityName1, 
                crimeFog1: crimeFog1, 
                crimeRain1: crimeRain1,
                crimeSnow1: crimeSnow1,
                crimeHail1: crimeHail1,
                crimeThunder1: crimeThunder1,
                crimeTornado1: crimeTornado1,
                crimeClear1: crimeClear1,
                communityName2: communityName2, 
                crimeFog2: crimeFog2, 
                crimeRain2: crimeRain2,
                crimeSnow2: crimeSnow2,
                crimeHail2: crimeHail2,
                crimeThunder2: crimeThunder2,
                crimeTornado2: crimeTornado2,
                crimeClear2: crimeClear2,
                costAvg1: costAvg1,
                timeAvg1: timeAvg1,
                frequency1: frequency1,
                costAvg2: costAvg2,
                timeAvg2: timeAvg2,
                frequency2: frequency2
            });

        })

    })

    // https://stackoverflow.com/questions/18008479/node-js-wait-for-multiple-async-calls
    /*
    var states = [{"State" : "NY"},{"State" : "NJ"}];

    var findLakes = function(state,callback){
        db.collection('lakes').find(state).toArray(callback);
    }

    async.map(states, findLakes , function(err, results){
        // do something with array of results
    });*/


    // Failed
    // Later codes were excuted before configs were collected.
    /*
    for (var key in obj){
        let [queryKey, hBaseTable] = obj[key];
        client.get(hBaseTable, queryKey, function(err, row){
            if (err) {
                res.render('index', {crime_taxi: null, error_taxi: null, crime_weather: null, error_weather: 'Some error just happened.'});
                return;
            } 
            configs[key] = row;
        })
    }*/

    // https://github.com/caolan/async
    // Failed
    // Later codes were excuted before configs were collected.
    /*
    async.forEachOf(obj, (value, key, callback) => {
        let [queryKey, hBaseTable] = value;
        configs[key] = 111;
        client.get(hBaseTable, queryKey, function(err, row){
            configs[key] = row;
        });callback();}, 
        err => {
            if (err) {console.error(err.message);}
            // configs is now a map of JSON data
            //doSomethingWith(configs);
        }
    );*/
    /*
    client.get(hbaseTableCrimeByWeather, community_pickup_key, function(err, row){
            configs["pickup"] = row;
            console.log(row);
        })
    client.get(hbaseTableCrimeByWeather, community_dropoff_key, function(err, row){
            configs["dropoff"] = row;
            console.log(row);
        })
    client.get(hbaseTableTaxiTrips, routeGo_key, function(err, row){
            configs["go"] = row;
            console.log(row);
        })
    client.get(hbaseTableTaxiTrips, routeBack_key, function(err, row){
            configs["back"] = row;
            console.log(row);
        })*/

    /*
    console.log(configs["pickup"], configs["dropoff"], configs["go"], configs["back"])

    function crimeAvg(row, weather) {
        var numCrimesBuffer = row.cols[`crime:${weather}_crime_sum`].value;
        var numCrimesBigInt = numCrimesBuffer.readIntBE(0, 8);
        var numCrimes = Number(numCrimesBigInt);

        var numDaysBuffer = row.cols[`crime:${weather}_days`].value;
        var numDaysBigInt = numDaysBuffer.readIntBE(0, 8);
        var numDays = Number(numDaysBigInt);

        var area = row.cols['crime:communityArea'].value;

        const scaleFactor = 1e8;
        if (numDays == 0) {
            return 'NULL';
        }
        else {
            return (numCrimes/(numDays*area)*scaleFactor).toFixed(1); // One decimal place
        }
    }

    const community_pickup_row = configs["pickup"]
    const community_dropoff_row = configs["dropoff"]
    const route_go_row = configs["go"]
    const route_back_row = configs["back"]

    var date_today = new Date();
    date_today.setHours(0,0,0,0)
    var date_first = new Date("2013-01-01");
    var date_diff = Math.floor((date_today-date_first) / (1000 * 3600 * 24));

    var communityName1 = community_pickup.toLowerCase().split(' ').map((s) => s.charAt(0).toUpperCase() + s.substring(1)).join(' ');
    var crimeFog1 = crimeAvg(community_pickup_row, 'fog');
    var crimeRain1 = crimeAvg(community_pickup_row, 'rain');
    var crimeSnow1 = crimeAvg(community_pickup_row, 'snow');
    var crimeHail1 = crimeAvg(community_pickup_row, 'hail');
    var crimeThunder1 = crimeAvg(community_pickup_row, 'thunder');
    var crimeTornado1 = crimeAvg(community_pickup_row, 'tornado');
    var crimeClear1 = crimeAvg(community_pickup_row, 'clear');


    var communityName2 = community_dropoff.toLowerCase().split(' ').map((s) => s.charAt(0).toUpperCase() + s.substring(1)).join(' ');
    var crimeFog2 = crimeAvg(community_dropoff_row, 'fog');
    var crimeRain2 = crimeAvg(community_dropoff_row, 'rain');
    var crimeSnow2 = crimeAvg(community_dropoff_row, 'snow');
    var crimeHail2 = crimeAvg(community_dropoff_row, 'hail');
    var crimeThunder2 = crimeAvg(community_dropoff_row, 'thunder');
    var crimeTornado2 = crimeAvg(community_dropoff_row, 'tornado');
    var crimeClear2 = crimeAvg(community_dropoff_row, 'clear');


    var routeDurationSumBuffer = route_go_row.cols['taxi:trip_duration_sum'].value;
    var routeDurationSumBigInt = routeDurationSumBuffer.readIntBE(0, 8);
    var routeDurationSum = Number(routeDurationSumBigInt);
    var totalCostSumBuffer = route_go_row.cols['taxi:total_cost_sum'].value;
    var totalCostSumBigInt = totalCostSumBuffer.readIntBE(0, 8);
    var totalCostSum = Number(totalCostSumBigInt);
    var numTripsBuffer = route_go_row.cols['taxi:num_trips'].value;
    var numTripsBigInt = numTripsBuffer.readIntBE(0, 8);
    var numTrips = Number(numTripsBigInt);
    var costAvg1 = (totalCostSum / numTrips).toFixed(1);
    var timeAvg1 = (routeDurationSum / numTrips).toFixed(1);
    var frequency1 = (numTrips / date_diff).toFixed(1);

    var routeDurationSumBuffer = route_back_row.cols['taxi:trip_duration_sum'].value;
    var routeDurationSumBigInt = routeDurationSumBuffer.readIntBE(0, 8);
    var routeDurationSum = Number(routeDurationSumBigInt);
    var totalCostSumBuffer = route_back_row.cols['taxi:total_cost_sum'].value;
    var totalCostSumBigInt = totalCostSumBuffer.readIntBE(0, 8);
    var totalCostSum = Number(totalCostSumBigInt);
    var numTripsBuffer = route_back_row.cols['taxi:num_trips'].value;
    var numTripsBigInt = numTripsBuffer.readIntBE(0, 8);
    var numTrips = Number(numTripsBigInt);
    var costAvg2 = (totalCostSum / numTrips).toFixed(1);
    var timeAvg2 = (routeDurationSum / numTrips).toFixed(1);
    var frequency2 = (numTrips / date_diff).toFixed(1);

    res.render('index', {crime_weather: null, error_weather: null, crime_taxi: true, error_taxi: null, 
        communityName1: communityName1, 
        crimeFog1: crimeFog1, 
        crimeRain1: crimeRain1,
        crimeSnow1: crimeSnow1,
        crimeHail1: crimeHail1,
        crimeThunder1: crimeThunder1,
        crimeTornado1: crimeTornado1,
        crimeClear1: crimeClear1,
        communityName2: communityName2, 
        crimeFog2: crimeFog2, 
        crimeRain2: crimeRain2,
        crimeSnow2: crimeSnow2,
        crimeHail2: crimeHail2,
        crimeThunder2: crimeThunder2,
        crimeTornado2: crimeTornado2,
        crimeClear2: crimeClear2,
        costAvg1: costAvg1,
        timeAvg1: timeAvg1,
        frequency1: frequency1,
        costAvg2: costAvg2,
        timeAvg2: timeAvg2,
        frequency2: frequency2
    });*/
});







/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
/* var kafkaClient = new kafka.KafkaClient({kafkaHost: '10.0.0.2:6667'}); */
var kafkaProducer = new Producer(kafkaClient);

app.get('/submit-crime', function (req, res) {
    // res.send('Hellow Underworld!');
    res.render('submit-crime', {success: null, error: null});
});


app.post('/submit-crime',function (req, res) {
    const caseNumber = req.body.caseNumber.trim();
    const community = req.body.community.trim().toLowerCase();
    const date = req.body.date;
    const dateInfo = date.split('-');
    const year = parseInt(dateInfo[0]);
    const month = parseInt(dateInfo[1]);
    const day = parseInt(dateInfo[2]);

    var date_today = new Date();
    date_today.setHours(0,0,0,0)
    var date_crime = new Date(date);
    var date_diff = Math.floor((date_today-date_crime) / (1000 * 3600 * 24));
    if (date_diff < 0) {
        res.render('submit-crime', {success: null, error: 'Could not submit future crime instance!'});
        return;
    }
    if (date_diff > 365) {
        res.render('submit-crime', {success: null, error: 'Crime instance too old to submit! Please submit crime instance within one year.'});
        return;
    }

    var crimeUpdate = {
        caseNumber : caseNumber,
        community : community,
        year : year,
        month : month,
        day : day
    };

    kafkaProducer.send([{topic: kafkaTopicCrime, messages: JSON.stringify(crimeUpdate)}],
        function (err, data) {
            if (err) {
                res.render('submit-crime', {success: null, error: 'Some error just happened.'});
            }
            else {
                res.render('submit-crime', {success: 'Crime update sent successfully.', error: null});
            }
        });

});


app.get('/submit-weather', function (req, res) {
    // res.send('Hellow Underworld!');
    res.render('submit-weather', {success: null, error: null});
});

app.post('/submit-weather',function (req, res) {
    const date = req.body.date.trim();
    const community = req.body.community.trim().toLowerCase();
    const dateInfo = date.split('-');
    const year = parseInt(dateInfo[0]);
    const month = parseInt(dateInfo[1]);
    const day = parseInt(dateInfo[2]);
    const fog = req.body.fog ? true : false;
    const rain = req.body.rain ? true : false;
    const snow = req.body.snow ? true : false;
    const hail = req.body.hail ? true : false;
    const thunder = req.body.thunder ? true : false;
    const tornado = req.body.tornado ? true : false;
    const dateString = dateInfo.join('');

    var date_today = new Date();
    date_today.setHours(0,0,0,0)
    var date_weather = new Date(date);
    var date_diff = Math.floor((date_today-date_weather) / (1000 * 3600 * 24));
    if (date_diff < 0) {
        res.render('submit-weather', {success: null, error: 'Could not submit future weather instance!'});
        return;
    }
    if (date_diff > 365) {
        res.render('submit-weather', {success: null, error: 'Weather instance too old to submit! Please submit weather instance within one year.'});
        return;
    }

    var weatherUpdate = {
        date : dateString,
        community : community,
        fog : fog,
        rain : rain,
        snow : snow,
        hail : hail,
        thunder : thunder,
        tornado : tornado
    };


    kafkaProducer.send([{topic: kafkaTopicWeather, messages: JSON.stringify(weatherUpdate)}],
        function (err, data) {
            if (err) {
                res.render('submit-weather', {success: null, error: 'Some error just happened.'});
            }
            else {
                res.render('submit-weather', {success: 'Weather update sent successfully.', error: null});
            }
        });

});

/* Use the following on VM */
app.listen(port, hostname, function () {
    console.log(`App starts to listen to http://${hostname}:${port}!`)
});

/* Use the following on Cloud */
/*
app.listen(port, function () {
    console.log(`App starts to listen to port ${port}!`)
});
*/
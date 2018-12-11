const express = require('express');
const bodyParser = require('body-parser');
const hbase = require('hbase-rpc-client');
//var Int64 = require('node-int64');
const app = express();
const hostname = '127.0.0.1';
const port = 3000;
const hbaseTable = 'leimao_community_crime_by_weather';
const kafkaTopicCrime = 'leimao_crime_update'
const kafkaTopicWeather = 'leimao_weather_update'

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
    res.render('index', {crime: null, error: null});
});


app.post('/', function (req, res) {
    // Chicago Community Information
    // https://en.wikipedia.org/wiki/Community_areas_in_Chicago
    // const community = 'hyde park';
    const community = req.body.community.trim().toLowerCase();
    const key = new hbase.Get(community);
    client.get(hbaseTable, key, function(err, row) {

        if (err) {
            res.render('index', {crime: null, error: 'Some error just happened.'});
            return;
        } 

        if (!row) {
            //res.send('<html><body>No such community in Chicago!</body></html>');
            res.render('index', {crime: null, error: 'No such community in Chicago or no crime found.'});
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
        res.render('index', {crime: true, error: null, communityName: communityName, crimeClear: crimeClear, crimeFog: crimeFog, crimeRain: crimeRain, crimeSnow: crimeSnow, crimeHail: crimeHail, crimeThunder: crimeThunder, crimeTornado: crimeTornado});
    })
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
const express = require('express');
const bodyParser = require('body-parser');
const hbase = require('hbase-rpc-client');
const app = express();
const hostname = '10.0.0.8';
const port = 3246;
const hbaseTable = 'leimao_community_crime_by_weather';

//app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.set('views', __dirname + '/views');
app.use(express.static(__dirname + '/public'));
app.set('view engine', 'ejs');

var client = hbase({
    /* Zookeeper information for VM */
    //zookeeperHosts: ['localhost:2181'],
    //zookeeperRoot: '/hbase'
    /* Zookeeper information for cluster */
    zookeeperHosts: ['10.0.0.2:2181'],
    zookeeperRoot: '/hbase-unsecure'
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
            res.render('index', {crime: null, error: 'No such community in Chicago.'});
            return;
        }

        function crimeAvg(weather) {
            var numCrimes = row.cols[`crime:${weather}_crime_sum`].value;
            var numDays = row.cols[`crime:${weather}_days`].value;
            var area = row.cols['crime:communityArea'].value;
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

/*
app.listen(port, hostname, function () {
    console.log(`App starts to listen to http://${hostname}:${port}!`)
});
*/

app.listen(port, function () {
    console.log(`App starts to listen to port ${port}!`)
});


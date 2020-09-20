var express = require("express");
var app = express();
var bodyParser = require("body-parser");
var request = require("request");
const port = 8080;
const fs = require("fs");

//let ukData = require('ltlas.json');
let ltlasData = fs.readFileSync('data/ltlas.json');
let ltlasJSON = JSON.parse(ltlasData);

let ltlasSumData = fs.readFileSync('data/ltlasSum.json');
let ltlasSumJSON = JSON.parse(ltlasSumData);

let ltlasWorst10Data = fs.readFileSync('data/ltlasWorst10Df.json');
let ltlasworst10JSON = JSON.parse(ltlasWorst10Data);

let ltlastop10CasesData = fs.readFileSync('data/ltlastop10last30d.json');
let ltlastop10CasesJSON = JSON.parse(ltlastop10CasesData);

let ltlasWorst10Rate30Data = fs.readFileSync('data/ltlasWorst10RateLast30D.json');
let ltlasWorst10Rate30JSON = JSON.parse(ltlasWorst10Rate30Data);

let ltlasAllSumDfData = fs.readFileSync('data/ltlasAllSumDf.json');
let ltlasAllSumDfJSON = JSON.parse(ltlasAllSumDfData);

let lastRefresh = fs.readFileSync('data/lastRefresh.json');
let lastRefreshJSON = JSON.parse(lastRefresh);

app.set("view engine", "ejs");
app.use(express.static("public"));

app.set('trust proxy', true);

app.get("/", function (req, res) {
    res.render("index.ejs", {
        ltlasworst10JSON: ltlasworst10JSON,
        ltlastop10CasesJSON: ltlastop10CasesJSON,
        ltlasWorst10Rate30JSON: ltlasWorst10Rate30JSON,
        lastRefreshJSON: lastRefreshJSON
    });
});

app.get("/summary", function (req, res) {
    res.render("summary.ejs", {
        ltlasAllSumDfJSON: ltlasAllSumDfJSON,
        lastRefreshJSON: lastRefreshJSON
    });
});


app.get("/results", function (req, res) {
    var adminDistrict = "";
    var query = req.query.postalCode;
    //    ukData = JSON.parse(ukData)
    query = query.replace(/\s/g, '');

    if (query.length < 9) {
        var url = "http://api.postcodes.io/postcodes/" + query;
        request(url, function (error, response, body) {
            if (!error && response.statusCode == 200) {
                var data = JSON.parse(body);
                var adminDistrict = data["result"]["codes"]["admin_district"];
                var filtered = ltlasJSON.filter(a => a.areaCode == adminDistrict);
                var filteredSum = ltlasAllSumDfJSON.filter(a => a.areaCode == adminDistrict);
                if (filtered.length > 0) {
                    let specimenDate = [];
                    let confirmedCases = [];
                    let ma7Lower = [];
                    for (i = 0; i < filtered.length; i++) {
                        date = new Date(filtered[i]['specimenDate'])
                        date = date.getFullYear() + '-' + (('0' + (date.getMonth() + 1)).slice(-2)) + '-' + (('0' + date.getDate()).slice(-2));
                        specimenDate.push(date);
                        confirmedCases.push(filtered[i]['dcLower']);
                        ma7Lower.push(filtered[i]['ma7Lower']);
                    }
                    areaName = filtered[0]['areaName'];
                    areaTotal = filteredSum[0]['dailyLabConfirmedCases'];
                    arealast30 = filteredSum[0]['last30dCases'];
                    areaR = filteredSum[0]['rBasic'];
                    arealast7 = filteredSum[0]['last7dCases'];
                    areaArea = filteredSum[0]['Area'];
                    areaPopulation = filteredSum[0]['Population'];
                    res.render("results", {
                        specimenDate: specimenDate,
                        confirmedCases: confirmedCases,
                        areaName: areaName,
                        areaTotal: areaTotal,
                        arealast30: arealast30,
                        areaR: areaR,
                        ma7Lower: ma7Lower,
                        arealast7: arealast7,
                        areaArea: areaArea,
                        areaPopulation: areaPopulation
                    });
                }
                else {
                    res.render("error");
                }
            }
            else {
                res.render("error");
            }
        }
        );
    }
    else if (query.length == 9) {
        var adminDistrict = query;
        var filtered = ltlasJSON.filter(a => a.areaCode == adminDistrict);
        var filteredSum = ltlasAllSumDfJSON.filter(a => a.areaCode == adminDistrict);
        if (filtered.length > 0) {
            let specimenDate = [];
            let confirmedCases = [];
            let ma7Lower = [];
            for (i = 0; i < filtered.length; i++) {
                date = new Date(filtered[i]['specimenDate'])
                date = date.getFullYear() + '-' + (('0' + (date.getMonth() + 1)).slice(-2)) + '-' + (('0' + date.getDate()).slice(-2));
                specimenDate.push(date);
                confirmedCases.push(filtered[i]['dcLower']);
                ma7Lower.push(filtered[i]['ma7Lower']);
            }
            areaName = filtered[0]['areaName'];
            areaTotal = filteredSum[0]['dailyLabConfirmedCases'];
            arealast30 = filteredSum[0]['last30dCases'];
            areaR = filteredSum[0]['rBasic'];
            arealast7 = filteredSum[0]['last7dCases'];
            areaArea = filteredSum[0]['Area'];
            areaPopulation = filteredSum[0]['Population'];
            res.render("results", {
                specimenDate: specimenDate,
                confirmedCases: confirmedCases,
                areaName: areaName,
                areaTotal: areaTotal,
                arealast30: arealast30,
                areaR: areaR,
                ma7Lower: ma7Lower,
                arealast7: arealast7,
                areaArea: areaArea,
                areaPopulation: areaPopulation                
            });
        }}
        else{
            res.render("Invalid Request!!!");
        }
    });



app.get("/search", function (req, res) {
    res.render("testSearch");
});

app.get("*", function (req, res) {
    res.render("error");
});

app.listen(process.port = port, function () {
    console.log("App has started!!!");
});

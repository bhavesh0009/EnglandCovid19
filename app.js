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

let ltlastop10last30dData = fs.readFileSync('data/ltlastop10last30d.json');
let ltlastop10last30dJSON = JSON.parse(ltlastop10last30dData);

app.set("view engine", "ejs");
app.use(express.static("public"));

app.set('trust proxy', true);

app.get("/", function (req, res) {
    let top10last30dArea = [];
    let top10last30dCases = [];
    for (i = 0; i < ltlastop10last30dJSON.length; i++) {
        top10last30dArea.push(ltlastop10last30dJSON[i]['area_name']);
        top10last30dCases.push(ltlastop10last30dJSON[i]['last30dCases']);
    }

    res.render("index.ejs", {
        top10last30dArea : top10last30dArea,
        top10last30dCases: top10last30dCases
    });
});

app.get("/results", function (req, res) {
    var query = req.query.postalCode;
    //    ukData = JSON.parse(ukData)
    query = query.replace(/\s/g, '');
    var url = "http://api.postcodes.io/postcodes/" + query;
    request(url, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            var data = JSON.parse(body);
            var adminDistrict = data["result"]["codes"]["admin_district"];
            var filtered = ltlasJSON.filter(a => a.areaCode == adminDistrict);
            var filteredSum = ltlasSumJSON.filter(a => a.areaCode == adminDistrict);
            if (filtered.length > 0) {
                let specimenDate = [];
                let confirmedCases = [];
                let upperConfirmedCases = [];
                let regionConfirmedCases = [];
                for (i = 0; i < filtered.length; i++) {
                    date = new Date(filtered[i]['specimenDate'])
                    date = date.getFullYear() + '-' + (('0' + (date.getMonth() + 1)).slice(-2)) + '-' + (('0' + date.getDate()).slice(-2));
                    specimenDate.push(date);
                    confirmedCases.push(filtered[i]['dcLower']);
                    upperConfirmedCases.push(filtered[i]['dcUpper']);
                    regionConfirmedCases.push(filtered[i]['dcRegion']);
                }
                areaName = filtered[0]['areaName'];
                upperName = filtered[0]['urName'];
                regionName = filtered[0]['rName'];
                areaTotal = filteredSum[0]['dailyLabConfirmedCases'];
                arealast30 = filteredSum[0]['last30dCases'];
                areaR = filteredSum[0]['rBasic'];
                res.render("results", {
                    specimenDate: specimenDate,
                    confirmedCases: confirmedCases,
                    areaName: areaName,
                    upperName: upperName,
                    regionName: regionName,
                    upperConfirmedCases: upperConfirmedCases,
                    regionConfirmedCases: regionConfirmedCases,
                    areaTotal: areaTotal,
                    arealast30: arealast30,
                    areaR: areaR
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
});

app.get("*", function (req, res) {
    res.render("error");
});

app.listen(process.port = port, function () {
    console.log("App has started!!!");
});

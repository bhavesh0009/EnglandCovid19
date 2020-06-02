var express = require("express");
var app = express();
var bodyParser = require("body-parser");
var request = require("request");
const port = 8080;
const fs = require("fs");

//let ukData = require('ltlas.json');
let ltlasData = fs.readFileSync('data/ltlas.json');
let ltlasJSON = JSON.parse(ltlasData);

app.set("view engine", "ejs");
app.use(express.static("public"));

app.set('trust proxy', true);

app.get("/", function(req, res){
    res.render("index.ejs");
});


app.get("/results", function(req, res){
    var query = req.query.postalCode;
//    ukData = JSON.parse(ukData)
    query = query.replace(/\s/g, '');
    var url = "http://api.postcodes.io/postcodes/" + query;
    request(url, function(error, response, body){
        if(!error && response.statusCode == 200){
            var data = JSON.parse(body);
            var adminDistrict = data["result"]["codes"]["admin_district"];
            var filtered = ltlasJSON.filter(a=>a.areaCode==adminDistrict);
            if(filtered.length>0){
                let specimenDate = [];
                let confirmedCases = [];
                let upperConfirmedCases = [];
                let regionConfirmedCases = [];
                for(i=0; i< filtered.length; i++){
                    date = new Date(filtered[i]['specimenDate'])
                    date = date.getFullYear()+'-' + (('0' + (date.getMonth()+ 1)).slice(-2)) + '-'+ (('0' + date.getDate()).slice(-2));
                    specimenDate.push(date);
                    confirmedCases.push(filtered[i]['dcLower']);
                    upperConfirmedCases.push(filtered[i]['dcUpper']);
                    regionConfirmedCases.push(filtered[i]['dcRegion']);
                }
                areaName = filtered[0]['areaName'];
                upperName = filtered[0]['urName'];
                regionName = filtered[0]['rName'];
                res.render("results", {specimenDate: specimenDate,
                                        confirmedCases: confirmedCases,
                                        areaName: areaName,
                                        upperName: upperName,
                                        regionName: regionName,
                                        upperConfirmedCases: upperConfirmedCases,
                                        regionConfirmedCases: regionConfirmedCases
                                     });
            }
            else{
                res.render("error");    
            }
        }
        else{
            res.render("error");
        }
    }
    );    
});

app.get("*", function(req, res){
    res.render("error");
});

app.listen(process.port=port, function(){
    console.log("App has started!!!");
});

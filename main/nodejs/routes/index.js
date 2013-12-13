var stress = require('./stress');

var AWS = require('aws-sdk');
/* Config for AWS */
AWS.config.loadFromPath('./aws.json');
AWS.config.update({region: 'us-west-2'});

var dynamodb = new AWS.DynamoDB();

exports.index = function(req, res){
  res.render('layout');
};

exports.query1 = function(req, res){
	res.render('query1');
};

exports.query2 = function(req, res){
	res.render('query2');
};

exports.query3 = function(req, res){
	res.render('query3');
};

exports.doQuery1 = function(req, res){
/* 
Given a phone-id and a date, return the sequence of cells visited by that phone on that day. 
*/

var query1 = {
	AttributesToGet: [
        "phonenumber","trace"
    ],
	Key: { 
		"phonenumber" : {
			"S" : req.query.pID
		},
		"date" : {
			"S" : req.query.date	
		}
	},
    TableName: "phonerecords"
};
	
	if(req.query.stress == 'true'){
		console.log('Starting Stress Test');
		stress.doStress(9999);		
		console.log('done');
	} 

dynamodb.getItem(query1, function (err, data) {
   if (err) {
   	console.log(err); // an error occurred
   } else {
   	res.send("Phone: " + data.Item.phonenumber.S + "  " + "Cells: " + data.Item.trace.SS);  
   }
});

};

exports.doQuery2 = function(req, res){
/*
Given a cell-id, a date and a time (only the beggining of the hour is accepted, e.g. 12:00),
list the phones present in that cell at that moment.
*/
var query2 = {
	AttributesToGet: [
        "phones","datetime"
    ],
	Key: { 
		"cellid" : {
			"S" : req.query.cID
		},
		"datetime" : {
			"S" : req.query.date + "|" + req.query.time
		}
	},
    TableName: "cellrecords"
};

	if(req.query.stress == 'true'){
		console.log('Starting Stress Test');
		stress.doStress(9999);
	}

dynamodb.getItem(query2, function (err, data) {
   if (err) {
     console.log(err); // an error occurred
   } else {
	res.send("Phones: " + data.Item.phones.SS );
   }
});  

};

exports.doQuery3 = function(req, res){
/*
Given a phone-id and a date, return the number of minutes that that phone was off the
network on that day.
*/
var query3 = {
	AttributesToGet: [
        "phonenumber","minutesonnet"
    ],
	Key: { 
		"phonenumber" : {
			"S" : req.query.pID
		},
		"date" : {
			"S" : req.query.date	
		}
	},
    TableName: "phonerecords"
};

	if(req.query.stress == 'true'){
		console.log('Starting Stress Test');
		stress.doStress(9999);
	}

dynamodb.getItem(query3, function (err, data) {
   if (err) {
     console.log(err); // an error occurred
   } else {
   var minutes = 60*24-data.Item.minutesonnet.N;
   res.send("Phone: " + data.Item.phonenumber.S + "  " + "Minutes off the Network: " + minutes);
   }
});
  
};
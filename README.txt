GRUPO 9

65964 Diogo Antunes
70614 João Azevedo


Hadoop:
	ValuesWritable (day;month;year;hours;minutes;eventID;data)
	MyDynamoDBOutputFormat
DynamoDB:
	2 tables:
		phonerecords (phonenumber,date,trace,minutes)
		cellrecords (cellid,datetime,phones

Web Application:
	- The three query's from the project
	- The system can be stressed by making a query and running multiple instance's of the website
		
Load Balancing:
	- CPU metrics	

> hadoop jar PhoneLogSystem.jar pt.ulisboa.tecnico.phonelog.mapreduce.PhoneLogMapReduce ./input/log_sample.txt

Hadoop version 2.2.0

http://ec2-54-201-36-47.us-west-2.compute.amazonaws.com/

PhoneLogSystem.jar was created by eclipse and contains all class dependencies.

GRUPO 9

65964 Diogo Antunes
70614 João Azevedo


Hadoop:
	ValuesWritable (day;month;year;hours;minutes;eventID;data)

DynamoDB:
	2 tables:
		phonerecords (phonenumber,date,trace,minutes)
		cellrecords (cellid,datetime,phones

Web Application:
	- The three query's from the project
	- The system can be stressed by making a query and running multiple instance's of the website
		
	

To do:
	- Change Hadoop--DynamoDB "connection"
	- Improve the quality of web app
	- Better design
	- Load balancer
	- Understand the best values for dynamodb provisioned throughput

> hadoop jar PhoneLog.jar PhoneLogMapReduce ../input/log_sample.txt ../output

http://ec2-54-201-36-47.us-west-2.compute.amazonaws.com/

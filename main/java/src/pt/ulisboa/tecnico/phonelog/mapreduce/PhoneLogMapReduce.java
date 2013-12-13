package pt.ulisboa.tecnico.phonelog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBConfiguration;
import com.willetinc.hadoop.mapreduce.dynamodb.io.*;

import pt.ulisboa.tecnico.phonelog.mapreduce.dynamodb.MyDynamoDBOutputFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * @author Diogo Antunes
 * @author João Azevedo
 *
 */
@SuppressWarnings("deprecation")
public class PhoneLogMapReduce {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, ValuesWritable> {

		private Text phoneKey = new Text();
		private ValuesWritable phoneData;

		private Text cellKey = new Text();
		private ValuesWritable cellData;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			switch (this.getEvent(value)) {
			// not necessarily relevant 
			case 0:
			case 1:
				this.handleJustCell(value);
				context.write(cellKey, cellData);
				break;
			case 2:
			case 3:
			case 8:
				this.handleValue(value);
				context.write(cellKey, cellData);
			case 4:
			case 5:
				this.handleJustPhone(value);
				context.write(phoneKey, phoneData);
				break;
			case 6:
			case 7:
			default:
				break;
			}

		}

		private int getEvent(Text value) {

			String[] pieces = value.toString().split(";");

			return Integer.parseInt(pieces[3]);

		}

		private void handleValue(Text value) {

			String[] pieces = value.toString().split(";");

			phoneKey.set(pieces[4]);
			phoneData = new ValuesWritable(pieces[1], pieces[2],
					Integer.parseInt(pieces[3]), pieces[0]);

			cellKey.set(pieces[0]);
			cellData = new ValuesWritable(pieces[1], pieces[2],
					Integer.parseInt(pieces[3]), pieces[4]);

		}


		private void handleJustCell(Text value) {

			String[] pieces = value.toString().split(";");

			cellKey.set(pieces[0]);
			cellData = new ValuesWritable(pieces[1], pieces[2],
					Integer.parseInt(pieces[3]), "");

		}

		private void handleJustPhone(Text value) {

			String[] pieces = value.toString().split(";");

			phoneKey.set(pieces[4]);
			phoneData = new ValuesWritable(pieces[1], pieces[2],
					Integer.parseInt(pieces[3]), pieces[0]);

		}

	}

	public static class PhoneCellReducer extends Reducer<Text,ValuesWritable,DynamoDBItemWritable,NullWritable> {

		private List<DynamoDBItemWritable> result = new ArrayList<DynamoDBItemWritable>();
		
		public void reduce(Text key, Iterable<ValuesWritable> values, Context context)
				throws IOException, InterruptedException {

			List<ValuesWritable> sortedValues = getSortedValues(values);			

		//	List<String> result = new ArrayList<String>();

			int orderNumber = 0;

			ValuesWritable initalValueOnNetwork = null;
			ValuesWritable lastValueOnNetwork = null;

			int minutes = 0;

			int leftNetwork = -1;

			if(this.keyIsPhone(key)) {

				SWritable phoneNumber = new SWritable("phonenumber", new AttributeValue(key.toString())) {};

				
				String currentDate = sortedValues.get(0).getDate();
				
				SWritable date = new SWritable("date", new AttributeValue(currentDate)) {};
						
				NWritable minutesOnNet;
				
				SSWritable finalTrace = null;
				
				List<String> trace = new ArrayList<String>();
				

				for (ValuesWritable val : sortedValues) {

					if(!val.getDate().equals(currentDate)) {

						if(leftNetwork == 0)
							minutes = (lastValueOnNetwork.getHours() - initalValueOnNetwork.getHours())*60 +
							(lastValueOnNetwork.getMinutes() - initalValueOnNetwork.getMinutes());

						finalTrace = new SSWritable("trace", new AttributeValue(trace)) {};
						
						trace.removeAll(trace);
						
						minutesOnNet = new NWritable("minutesonnet", new AttributeValue(minutes + "")) {};
						
						result.add(orderNumber, new DynamoDBItemWritable(phoneNumber, date, minutesOnNet, finalTrace) {});
						
						orderNumber++;
						
						date = new SWritable("date", new AttributeValue(val.getDate())) {};
						
						currentDate = val.getDate();
						
					}

					switch (val.getEventID()) {
					case 2:
						
						trace.add(val.getData());
						
						break;
					case 3:
						break;
					case 4:
						initalValueOnNetwork = val;
						lastValueOnNetwork = val;
						leftNetwork = 0;
						break;
					case 5:
						lastValueOnNetwork = val;

						minutes = (lastValueOnNetwork.getHours() - initalValueOnNetwork.getHours())*60 +
								(lastValueOnNetwork.getMinutes() - initalValueOnNetwork.getMinutes());

						leftNetwork = 1;
						break;
					case 8:
						lastValueOnNetwork = val;
						break;

					default:
						break;
					}


				}


				if(leftNetwork == 0)
					minutes = (lastValueOnNetwork.getHours() - initalValueOnNetwork.getHours())*60 +
					(lastValueOnNetwork.getMinutes() - initalValueOnNetwork.getMinutes());
				
				finalTrace = new SSWritable("trace", new AttributeValue(trace)) {};

				minutesOnNet = new NWritable("minutesonnet", new AttributeValue(minutes + "")) {};
				
				result.add(orderNumber, new DynamoDBItemWritable(phoneNumber, date, minutesOnNet, finalTrace) {});
				

			} else {
				
				SWritable cellID = new SWritable("cellid", new AttributeValue(key.toString())) {};

				int currentHour = sortedValues.get(0).getHours() + 1;

				List<String> insideCell = new ArrayList<String>();
				
				List<String> insideCellControl = new ArrayList<String>();

				SWritable dateTime = new SWritable("datetime",
								new AttributeValue(upperDateTime(sortedValues.get(0)))) {};
				
				for (ValuesWritable val : sortedValues) {

					switch (val.getEventID()) {

					case 2:

						insideCellControl.add(val.getData());

					case 8:

						int valid = 0;
						for (String s : insideCellControl)
							if (s.equals(val.getData()))
								valid = 1;
						if (valid == 0)
							break;

						if(currentHour != ((val.getHours()+1)%24)) {

							currentHour = ((val.getHours() + 1) % 24);
							
							SSWritable phones = new SSWritable("phones", new AttributeValue(insideCell)) {};
							
							result.add(orderNumber, new DynamoDBItemWritable(cellID, dateTime, phones) {});
							
							orderNumber++;

							dateTime = new SWritable("datetime", new AttributeValue(upperDateTime(val))) {};
							
							insideCell.removeAll(insideCell);
							
							insideCell.add(val.getData());
							
						} else {

							if(!insideCell.contains(val.getData()))
								insideCell.add(val.getData());
							
						}

						break;

					case 3:

						if(!insideCell.contains(val.getData()))
							insideCell.add(val.getData());
						
						for (String s : insideCellControl)
							if (s.equals(val.getData()))
								insideCellControl.remove(val.getData());
						
						break;

					default:
						break;
					}

				}
				
				
				if (!insideCell.isEmpty()) {
					SSWritable phones = new SSWritable("phones", new AttributeValue(insideCell)) {};
				
					result.add(orderNumber, new DynamoDBItemWritable(cellID, dateTime, phones) {});
				
				}					
			
			}


			for (DynamoDBItemWritable record : result)
				context.write(record, NullWritable.get());
			

		}
		

		private boolean keyIsPhone(Text key) {  

			try {  	
				Double.parseDouble(key.toString());  

			} catch(NumberFormatException nfe) {  

				return false;  
			}

			return true;  
		}

		private String upperDateTime(ValuesWritable value) {

			if (value.getMinutes() == 0)

				return value.dateTime();

			else {
				
				Calendar cal = new GregorianCalendar(value.getYear(), value.getMonth(),
						value.getDay(), value.getHours(), 0);
				
				System.out.println(cal.get(Calendar.DAY_OF_MONTH) + "-" + 
						cal.get(Calendar.MONTH) + "-" + cal.get(Calendar.YEAR) 
						+ "|" + cal.get(Calendar.HOUR) + ":" + cal.get(Calendar.MINUTE));

				
				cal.add(Calendar.HOUR_OF_DAY, 1);
				
				System.out.println(cal.get(Calendar.DAY_OF_MONTH) + "-" + 
						cal.get(Calendar.MONTH) + "-" + cal.get(Calendar.YEAR) 
						+ "|" + cal.get(Calendar.HOUR) + ":" + cal.get(Calendar.MINUTE));

				return cal.get(Calendar.DAY_OF_MONTH) + "-" + 
						cal.get(Calendar.MONTH) + "-" + cal.get(Calendar.YEAR) 
						+ "|" + cal.get(Calendar.HOUR) + ":" + cal.get(Calendar.MINUTE);

			}
		}

		private ArrayList<ValuesWritable> getSortedValues(Iterable<ValuesWritable> values) {

			ArrayList<ValuesWritable> copy = new ArrayList<ValuesWritable>();

			for(ValuesWritable val : values)
				copy.add(new ValuesWritable(val.getDay(),val.getMonth(),
						val.getYear(),val.getHours(),val.getMinutes(),
						val.getEventID(),val.getData()));


			Collections.sort(copy);

			return copy;

		}


	}


	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
		
		conf.set(DynamoDBConfiguration.ACCESS_KEY_PROPERTY, "AKIAJG3MMHEHTFU2SQCA");
		conf.set(DynamoDBConfiguration.SECRET_KEY_PROPERTY, "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");
		conf.set(DynamoDBConfiguration.DYNAMODB_ENDPOINT, "dynamodb.us-west-2.amazonaws.com");
		conf.set(DynamoDBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, "cellrecords-phonerecords");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "Phone Log System");
		job.setJarByClass(PhoneLogMapReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(PhoneCellReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ValuesWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		job.setOutputFormatClass(MyDynamoDBOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

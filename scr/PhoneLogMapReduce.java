

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author Diogo Antunes
 * @author João Azevedo
 *
 */
public class PhoneLogMapReduce {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, ValuesWritable> {

		private Text phoneKey = new Text();
		private ValuesWritable phoneData;

		private Text cellKey = new Text();
		private ValuesWritable cellData;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			switch (this.getEvent(value)) {
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

	public static class PhoneCellReducer extends Reducer<Text,ValuesWritable,Text,Text> {

		private DynamoDBHandler dynamo = new DynamoDBHandler("phonerecords", "cellrecords");

		private Text result = new Text("");

		public void reduce(Text key, Iterable<ValuesWritable> values, Context context)
				throws IOException, InterruptedException {

			List<ValuesWritable> sortedValues = getSortedValues(values);			

			List<String> result = new ArrayList<String>();

			int orderNumber = 0;

			ValuesWritable initalValueOnNetwork = null;
			ValuesWritable lastValueOnNetwork = null;

			int minutes = 0;

			int leftNetwork = -1;


			if(this.keyIsPhone(key)) {

				String currentDate = sortedValues.get(0).getDate();

				result.add(orderNumber, currentDate + ",");

				for (ValuesWritable val : sortedValues) {

					if(!val.getDate().equals(currentDate)) {

						if(leftNetwork == 0)
							minutes = (lastValueOnNetwork.getHours() - initalValueOnNetwork.getHours())*60 +
							(lastValueOnNetwork.getMinutes() - initalValueOnNetwork.getMinutes());

						result.set(orderNumber, result.get(orderNumber) + "\b," + minutes) ;

						orderNumber++;
						currentDate = val.getDate();
						result.add(orderNumber, currentDate + ",");

					}

					switch (val.getEventID()) {
					case 2:
						result.set(orderNumber, result.get(orderNumber) + val.getData() + ";");

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

				result.set(orderNumber, result.get(orderNumber) + "\b," + minutes) ;



			} else {

				int currentHour = sortedValues.get(0).getHours();

				result.add(orderNumber, sortedValues.get(0).getDate() + "|" + currentHour + ":00,");
				
				for (ValuesWritable val : sortedValues) {

					switch (val.getEventID()) {
					
					case 2:
					case 8:
						
						if(currentHour != val.getHours()) {
				
							currentHour = val.getHours();
							orderNumber++;
							
							result.add(orderNumber, sortedValues.get(0).getDate() +
																"|" + currentHour + ":00,");
						}
						
						result.set(orderNumber, result.get(orderNumber) + val.getData() + ";");
						
						break;
						
					case 3:
						
						String aux = result.get(orderNumber).split(",")[0] + ",";
						
						for (String s :result.get(orderNumber).split(",")[1].split(";"))
							if(!s.equals(val.getData()))
								aux += s + ";";
						
						result.set(orderNumber, aux);
						
						break;

					default:
						break;
					}

				}				

			}




			String joinString = "";


			for (String s : result)
				joinString += s + "\n";


			this.result.set(joinString);

			for (String s : result)				
				dynamo.write(key, new Text(s));


			context.write(key, this.result);
		}

		private boolean keyIsPhone(Text key) {  

			try {  	
				Double.parseDouble(key.toString());  

			} catch(NumberFormatException nfe) {  

				return false;  
			}

			return true;  
		}

		private String getDate(ValuesWritable value) {

			return new DecimalFormat("00").format(value.getDay()) + "-" +
					new DecimalFormat("00").format(value.getMonth()) + "-" +
					new DecimalFormat("0000").format(value.getYear());
		}

		private String getTime(ValuesWritable value) {

			return new DecimalFormat("00").format(value.getHours()) + ":" +
					new DecimalFormat("00").format(value.getMinutes());
		}
		
		@SuppressWarnings("deprecation")
		private String upperDateTime(ValuesWritable value) {
			
			if (value.getMinutes() == 0)
		
				return value.dateTime();
			
			else {
				
				Calendar cal = Calendar.getInstance();
			    cal.setTime(new Date(value.getYear(), value.getMonth(),
			    				value.getDay(), value.getHours(), value.getMinutes()));
			    cal.add(Calendar.HOUR_OF_DAY, 1);
			    Date d = cal.getTime(); 

			    return d.getDay() + "-" + d.getMonth() + "-" + d.getYear() 
			    			+ "|" + d.getHours() + ":" + d.getMinutes();
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

		private int pathOrderNumber(List<VisitedItem> path, String cellID) {

			for (VisitedItem item : path)
				if (item.getName() == cellID)
					return item.orderNumber();

			return -1;

		}

		private VisitedItem findItem(List<VisitedItem> path, String cellID) {

			for (VisitedItem item : path)
				if (item.getName().equals(cellID))
					return item;

			return null;
		}

		private void findAndUpdatePingedItem(List<VisitedItem> path, String cellID, String leaveDateTime) {

			for (VisitedItem item : path)
				if (item.getName().equals(cellID+"-cell") && !item.isFinished())
					item.setLeaveDateTime(leaveDateTime);
				else
					if (item.getName().equals(cellID+"-network") && !item.isFinished())
						item.setLeaveDateTime(leaveDateTime);
		}

	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Phone Log");
		job.setJarByClass(PhoneLogMapReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		//	job.setCombinerClass(PhoneCellReducer.class);
		job.setReducerClass(PhoneCellReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ValuesWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

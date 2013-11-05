
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
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

			if (this.getEvent(value) != 0 && this.getEvent(value) != 1) {

				this.handleValue(value);

				context.write(phoneKey, phoneData);
				context.write(cellKey, cellData);

			} else {

				this.handleJustCell(value);
				context.write(cellKey, cellData);

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

	}

	public static class PhoneCellReducer extends Reducer<Text,ValuesWritable,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<ValuesWritable> values, Context context)
				throws IOException, InterruptedException {

			List<ValuesWritable> copy = new ArrayList<ValuesWritable>();

			for(ValuesWritable val : values)
				copy.add(new ValuesWritable(val.getDay(),val.getMonth(),
							val.getYear(),val.getHours(),val.getMinutes(),
								val.getEventID(),val.getData()));
			
			System.out.println("init:");
			
			for(ValuesWritable val : copy)
				System.out.println(val);
			
			Collections.sort(copy);
			
			System.out.println("after:");
			
			for(ValuesWritable val : copy)
				System.out.println(val);
			
			String result = "";

			if(this.keyIsPhone(key)) {

				System.out.println(key.toString());

				for (ValuesWritable val : copy) {

/*					try {
						
						Date date = new SimpleDateFormat("d-m-y").parse(val.toString());
					
					} catch (ParseException e) {
						
						System.out.println("Data Related Problems");
					}
	*/				
					result = result + "date: " + getDate(val) + " cell: " + val.getData() + 
							" event: " + val.getEventID() + " time: " + getTime(val) + "\n";

				}

			} else {



			}

			this.result.set(result);

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

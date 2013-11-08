import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


	public class DynamoDBOutputFormat<K, V> extends OutputFormat<K, V> {

		@Override
		public void checkOutputSpecs(JobContext arg0) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RecordWriter<K, V> getRecordWriter(TaskAttemptContext arg0)
				throws IOException, InterruptedException {

			return new DynamoDBRecordWriter<K, V>();
		}
	}
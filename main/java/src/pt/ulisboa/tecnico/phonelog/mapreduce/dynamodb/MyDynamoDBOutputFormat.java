
package pt.ulisboa.tecnico.phonelog.mapreduce.dynamodb;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBConfiguration;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBOutputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBItemWritable;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

/**
 * @author Diogo Antunes
 * @author Joao Azevedo
 * @version 2.0
 * 
 * Class that extends DynamoDBOutputFormat.
 *
 * @param <K> DynamoDBKeyWritable
 * @param <V> NullWritable
 */
@SuppressWarnings("deprecation")
public class MyDynamoDBOutputFormat<K extends DynamoDBKeyWritable, V> extends
		DynamoDBOutputFormat<K, V> {

	/* (non-Javadoc)
	 * @see com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBOutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public void checkOutputSpecs(JobContext context)
			throws IOException,
			InterruptedException {

	}

	/* (non-Javadoc)
	 * @see com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBOutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException,
			InterruptedException {
		return new FileOutputCommitter(
				FileOutputFormat.getOutputPath(context),
				context);
	}

	/**
	 * The Class MyDynamoDBRecordWriter.
	 */
	public class MyDynamoDBRecordWriter extends RecordWriter<K, V> {

		/** The client. */
		private AmazonDynamoDBClient client;

		/** The phones table. */
		private String phonesTable;
		
		/** The cells table. */
		private String cellsTable;


		/**
		 * Instantiates a new my dynamo db record writer.
		 */
		public MyDynamoDBRecordWriter() {}

		/**
		 * Instantiates a new my dynamo db record writer for two tables.
		 *
		 * @param client the client
		 * @param tableName the table name
		 */
		public MyDynamoDBRecordWriter(AmazonDynamoDBClient client,
				String tableName) {
			this.client = client;
			
			String[] tables = tableName.split("-");
			
			this.cellsTable = tables[0];
			this.phonesTable = tables[1];
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public void close(TaskAttemptContext context)
				throws IOException,
				InterruptedException {
			if (null != client) {
				client.shutdown();
			}
		}
		
		/**
		 * Gets the client.
		 *
		 * @return the client
		 */
		public AmazonDynamoDBClient getClient() {
			return client;
		}

		/**
		 * Gets the phones table.
		 *
		 * @return the phones table
		 */
		public String getPhonesTable() {
			return phonesTable;
		}

		/**
		 * Gets the cells table.
		 *
		 * @return the cells table
		 */
		public String getCellsTable() {
			return cellsTable;
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
		 */
		@Override
		public void write(K key, V value)
				throws IOException,
				InterruptedException {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			key.write(item);
			
			key.getHashKey();
			key.getRangeKey();
			
			Map<String, AttributeValueUpdate> map = new HashMap<String, AttributeValueUpdate>();
			
			UpdateItemRequest updateItemRequest = null;
			
			/* Different treatment for cell or phone and does updates
			 * instead of puts (update does put if the item does not exist) */
			if(key.getHashKey().getFieldName().equals("cellid")){
			
				map.put("phones", new AttributeValueUpdate(((DynamoDBItemWritable) key).get(2), AttributeAction.ADD));
			
				updateItemRequest = new UpdateItemRequest(cellsTable,
												new Key(key.getHashKeyValue(), key.getRangeKeyValue()) , map);
			
				client.updateItem(updateItemRequest);
				
			} else if(key.getHashKey().getFieldName().equals("phonenumber")) {
				
				map.put("minutesonnet", new AttributeValueUpdate(((DynamoDBItemWritable) key).get(2), AttributeAction.PUT));
				map.put("trace", new AttributeValueUpdate(((DynamoDBItemWritable) key).get(3), AttributeAction.ADD));
				
				updateItemRequest = new UpdateItemRequest(phonesTable,
												new Key(key.getHashKeyValue(), key.getRangeKeyValue()) , map);
			
				client.updateItem(updateItemRequest);	
			}
		}

	}
	
	/* (non-Javadoc)
	 * @see com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException,
			InterruptedException {
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(
				context.getConfiguration());
		return new MyDynamoDBRecordWriter(
				dbConf.getAmazonDynamoDBClient(),
				dbConf.getOutputTableName());
	}

}

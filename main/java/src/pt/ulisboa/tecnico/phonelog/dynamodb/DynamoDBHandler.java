package pt.ulisboa.tecnico.phonelog.dynamodb;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.PutItemRequest;

/**
 * @author Diogo Antunes
 * @author Joao Azevedo
 * @version 1.0
 * 
 * The Class DynamoDBHandler.
 */
@Deprecated
public class DynamoDBHandler {

	/** The dynamo db. */
	static AmazonDynamoDBClient dynamoDB;

	/** The phones table. */
	private String phonesTable;
	
	/** The cells table. */
	private String cellsTable;

	/**
	 * Instantiates a new dynamo db handler.
	 *
	 * @param phonesTable the phones table
	 * @param cellsTable the cells table
	 */
	public DynamoDBHandler(String phonesTable, String cellsTable) {
		
		dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("AKIAJG3MMHEHTFU2SQCA", "Le5C2SbB/luAPwND4UgRtBmBv64+LARm7KesxuZU"));
		dynamoDB.setEndpoint("dynamodb.us-west-2.amazonaws.com");
		
		this.phonesTable = phonesTable;
		this.cellsTable = cellsTable;
	};

	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public AmazonDynamoDBClient getClient() {
		return dynamoDB;
	}

	/**
	 * Write.
	 *
	 * @param key the key
	 * @param result the result
	 */
	public void write(Text key, Text result) {

		Map<String, AttributeValue> item;
		PutItemRequest putItemRequest;
		
		if (keyIsPhone(key.toString())) {

			item = newPhoneItem(key.toString(), result.toString().split(",")[0],
					Integer.parseInt(result.toString().split(",")[2]), result.toString().split(",")[1]);
			putItemRequest = new PutItemRequest(phonesTable, item);

		} else {

			item = newCellItem(key.toString(), result.toString().split(",")[0],
					result.toString().split(",")[1]);
			putItemRequest = new PutItemRequest(cellsTable, item);

		}

		try {
			
		//PutItemResult putItemResult = 
		dynamoDB.putItem(putItemRequest);
		//System.out.println("Result: " + putItemResult);

		} catch (IllegalStateException e) {	}

	}


	/**
	 * New phone item.
	 *
	 * @param phoneNumber the phone number
	 * @param date the date
	 * @param minutesOnNet the minutes on net
	 * @param trace the trace
	 * @return the map
	 */
	private static Map<String, AttributeValue> newPhoneItem(String phoneNumber, String date, int minutesOnNet, String trace) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("phonenumber", new AttributeValue(phoneNumber));
		item.put("date", new AttributeValue(date));
		item.put("minutesonnet", new AttributeValue().withN(Integer.toString(minutesOnNet)));
		item.put("trace", new AttributeValue().withSS(trace.split(";")));

		return item;
	}

	/**
	 * New cell item.
	 *
	 * @param cellID the cell id
	 * @param dateTime the date time
	 * @param phones the phones
	 * @return the map
	 */
	private static Map<String, AttributeValue> newCellItem(String cellID, String dateTime, String phones) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("cellid", new AttributeValue(cellID));
		item.put("datetime", new AttributeValue(dateTime));
		item.put("phones", new AttributeValue().withSS(phones.split(";")));

		return item;
	}

	/**
	 * Key is phone.
	 *
	 * @param string the string
	 * @return true, if successful
	 */
	private boolean keyIsPhone(String string) {  

		try {  	
			Double.parseDouble(string);  

		} catch(NumberFormatException nfe) {  

			return false;  
		}

		return true;  
	}

}

package org.example;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */
public class DynamoDBTest {

	private final DynamoDBConnectionManager connMgr_ = new DynamoDBConnectionManager();
	private static final Logger logger = LoggerFactory.getLogger(DynamoDBTest.class);

	public DynamoDBTest() {

	}

	public DynamoDBConnectionManager getConnectionManager() {
		return connMgr_;
	}

	public DynamoDBConnection getDynamoConnection() {
		return this.getConnectionManager().getPrimaryConnection();
	}

	public String getConnectionName() {
		DynamoDBConnection connection = this.getDynamoConnection();
		return (connection == null) ? "unknown" : connection.getClientEndpointName();
	}

	public static void main(String[] args) {

		DynamoDBTest tester = new DynamoDBTest();
		
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
		
		if (tester.getDynamoConnection() == null) {
			logger.info("Could not establish connection");
			//System.exit(1);
		}

		
		logger.debug("Using connection: " + tester.getConnectionName());

		int testRuns = 50;
		int numOfRuns = 0;
		while (numOfRuns < testRuns) {
			try {
				numOfRuns++;
				logger.debug(String.format("Test run number %n", numOfRuns));
				runTests(tester, numOfRuns);
				Thread.sleep(5000);
			} catch (InterruptedException ex) {
			}
		}
	}

	public static void runTests(DynamoDBTest tester, int numOfRuns) {
		// List<String> l = tester.listAllTables();
		// tester.describeDymamoDBTables(l);
		String key = tester.testPut(numOfRuns);
		tester.testGet(key);
	}

	public void describeDymamoDBTables(List<String> tableNames) {

		logger.info("Listing your Amazon DynamoDB tables:\n");

		DynamoDBConnection connection = this.getDynamoConnection();
		if (connection == null) {
			logger.info("No available connections");
			return;
		}

		for (String tableName : tableNames) {

			try {
				DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();

				TableDescription tableInfo = connection.describeTable(request);
				if (tableInfo != null) {
					logger.info("Table Summary");
					logger.info(String.format("  Table name  : %s\n", tableInfo.tableName()));
					logger.info(String.format("  Table ARN   : %s\n", tableInfo.tableArn()));
					logger.info(String.format("  Status      : %s\n", tableInfo.tableStatus()));
					logger.info(String.format("  Item count  : %d\n", tableInfo.itemCount()));
					logger.info(String.format("  Size (bytes): %d\n", tableInfo.tableSizeBytes()));

					ProvisionedThroughputDescription throughputInfo = tableInfo.provisionedThroughput();
					logger.info("Throughput");
					logger.info(String.format("  Read Capacity : %d\n", throughputInfo.readCapacityUnits()));
					logger.info(String.format("  Write Capacity: %d\n", throughputInfo.writeCapacityUnits()));

					List<AttributeDefinition> attributes = tableInfo.attributeDefinitions();
					logger.info("Attributes");
					for (AttributeDefinition a : attributes) {
						logger.info(String.format("  %s (%s)\n", a.attributeName(), a.attributeType()));
					}
				}

			} catch (DynamoDbException e) {
				System.err.println("Error describing [" + tableName + "]: " + e.getMessage());
			}
		}
		System.out.println("\nDone!");
	}

	public List<String> listAllTables() {

		DynamoDBConnection connection = this.getDynamoConnection();

		List<String> tableNames = new ArrayList<>();

		if (connection == null) {
			logger.info("No available connections");
			return tableNames;
		}

		try {
			
			ListTablesRequest request = ListTablesRequest.builder().build();
			tableNames = connection.listTables(request);
			if (tableNames.size() > 0) {
				for (String curName : tableNames) {
					logger.info("* %s\n", curName);
				}
			} else {
				logger.info("No tables found!");
			}

		} catch (DynamoDbException e) {
			logger.error("Could not list tables: " + e.getMessage());
		}
		logger.info("\n Table Name List Done!");
		return tableNames;
	}

	public String testPut(int numOfRuns) throws DynamoDbException {

		String tableName = "Person";
		String key = "last-name";
		String keyVal = String.format("Scriffiny-%02d", numOfRuns);
		String sortKey = "first-name";
		String sortKeyVal = "Patty";

		putItemInTable(tableName, key, keyVal, sortKey, sortKeyVal);

		return keyVal;

	}

	public void testGet(String keyVal) throws DynamoDbException {

		DynamoDBConnection connection = this.getDynamoConnection();

		if (connection == null) {
			logger.info("No available connections");
			return;
		}

		String key = "last-name";
		String sortKey = "first-name";
		String sortKeyVal = "Patty";
		String tableName = "Person";

		HashMap<String, AttributeValue> keyToGet = new HashMap<>();
		keyToGet.put(key, AttributeValue.builder().s(keyVal).build());

		keyToGet.put(sortKey, AttributeValue.builder().s(sortKeyVal).build());

		GetItemRequest request = GetItemRequest.builder().key(keyToGet).tableName(tableName).build();

		try {
			Map<String, AttributeValue> returnedItem = connection.getItem(request).item();
			if (returnedItem.isEmpty())
				logger.info(String.format("No item found with the key %s!\n", key));
			else {
				Set<String> keys = returnedItem.keySet();

				logger.info(
						String.format("Amazon DynamoDB Read Result %s/%s: \n", this.getConnectionName(), tableName));
				for (String key1 : keys) {
					logger.info(String.format("   %s: %s\n", key1, returnedItem.get(key1).toString()));
				}
			}
			logger.info(tableName + " was successfully read.");

		} catch (ResourceNotFoundException e) {
			logger.error("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			logger.error("Be sure that it exists and that you've typed its name correctly!");
		} catch (DynamoDbException e) {
			logger.error("Could not read :" + e.getMessage());

		}

	}

	private void putItemInTable(String tableName, String key, String keyVal, String sortKey, String sortKeyVal)
			throws DynamoDbException {

		DynamoDBConnection connection = this.getDynamoConnection();

		if (connection == null) {
			logger.info("No available connections");
			return;
		}

		HashMap<String, AttributeValue> itemValues = new HashMap<>();
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(sortKey, AttributeValue.builder().s(sortKeyVal).build());
		itemValues.put("timestamp", AttributeValue.builder().s(new Date().toString()).build());
		itemValues.put("region", AttributeValue.builder().s(this.getConnectionName()).build());

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			PutItemResponse response = connection.putItem(request);
			logger.info(tableName + " was successfully updated");

		} catch (ResourceNotFoundException e) {
			logger.error("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			logger.error("Be sure that it exists and that you've typed its name correctly!");
		} catch (DynamoDbException e) {
			logger.error("Cound not write :" + e.getMessage());

		}

		logger.info(String.format("Put Item in %s/" + tableName + " Done!", this.getConnectionName()));
	}

}

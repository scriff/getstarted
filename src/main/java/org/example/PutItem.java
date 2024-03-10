package org.example;

import java.util.Date;
import java.util.HashMap;
//snippet-end:[dynamodb.java2.put_item.import]

//snippet-start:[dynamodb.java2.put_item.main]
//snippet-start:[dynamodb.java2.put_item.import]
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 *
 * To place items into an Amazon DynamoDB table using the AWS SDK for Java V2,
 * its better practice to use the Enhanced Client. See the EnhancedPutItem
 * example.
 */
public class PutItem {
	public static void main(String[] args) {

		String tableName = "Person";
		String key = "last-name";
		String keyVal = "Scriffiny";
		String sortKey = "first-name";
		String sortKeyVal = "Patty";
		Region region = Region.US_EAST_2;
		DynamoDbClient ddb = DynamoDbClient.builder().region(region).build();

		putItemInTable(ddb, tableName, key, keyVal, sortKey, sortKeyVal);
		System.out.println("Done!");
		ddb.close();
	}

	public static void putItemInTable(DynamoDbClient ddb, String tableName, String key, String keyVal, String sortKey,
			String sortKeyVal) {

		HashMap<String, AttributeValue> itemValues = new HashMap<>();
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(sortKey, AttributeValue.builder().s(sortKeyVal).build());
		itemValues.put("timestamp", AttributeValue.builder().s(new Date().toString()).build());

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			PutItemResponse response = ddb.putItem(request);
			System.out.println(tableName + " was successfully updated. The request id is "
					+ response.responseMetadata().requestId());

		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
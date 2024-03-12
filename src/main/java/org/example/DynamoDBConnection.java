package org.example;

import java.net.URI;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

public class DynamoDBConnection {
	private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnection.class);

	private DynamoDbClient dynamoDBClient_;
	final Region region_;
	private boolean isPrimary_ = false;

	public DynamoDBConnection(Region region) {
		region_ = region;
	}

	public boolean isPrimary() {
		if (dynamoDBClient_ == null)
			this.createConnection();
		return isPrimary_;
	}

	public void closeConnection() {
		try {
			if (dynamoDBClient_ != null)
				dynamoDBClient_.close();

			dynamoDBClient_ = null;
		} catch (Exception ex) {
			logger.error("Error closing DDB Client: " + ex);
		}
	}

	public DynamoDbClient getConnection() {
		if (dynamoDBClient_ == null)
			createConnection();

		return dynamoDBClient_;
	}

	private void createConnection() {
		try {
			if (dynamoDBClient_ != null)
				dynamoDBClient_.close();

			DynamoDbClientBuilder builder = DynamoDbClient.builder();
			
			URI uri = new URI("https://dynamodb.us-west-2.amazonaws.com");

			dynamoDBClient_ = builder.region(region_).build();
			//dynamoDBClient_ = builder.endpointOverride(uri).build();
					
			this.setIsPrimary();
		} catch (Exception ex) {
			logger.error("Error creating DDB Client" + ex);
		}
	}

	private void setIsPrimary() {

		logger.info(String.format("Checking %s for primary", region_.toString()));

		String tableName = "Person";
		HashMap<String, AttributeValue> itemValues = new HashMap<>();
		itemValues.put("last-name", AttributeValue.builder().s(dynamoDBClient_.describeEndpoints().toString()).build());
		itemValues.put("first-name", AttributeValue.builder().s(region_.toString()).build());
		itemValues.put("timestamp", AttributeValue.builder().s(new Date().toString()).build());

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			PutItemResponse response = this.getConnection().putItem(request);
			logger.info(tableName + " was successfully updated. The request id is "
					+ response.responseMetadata().requestId());
			isPrimary_ = true;

		} catch (DynamoDbException e) {

			AwsErrorDetails errorDetails = e.awsErrorDetails();
			isPrimary_ = !errorDetails.errorCode().startsWith("AccessDenied");
			logger.error(e.getMessage());

		}

		logger.info(String.format("Region %s primary is set to %s", region_.toString(), isPrimary_));
	}

	@Override
	public String toString() {
		return String.format("DynamoDBConnection [Region: %s, isPrimary: %s]", region_.toString(), isPrimary_);
	}
}

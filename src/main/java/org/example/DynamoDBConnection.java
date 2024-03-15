package org.example;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.Endpoint;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.paginators.ListTablesIterable;

public class DynamoDBConnection {
	private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnection.class);

	private DynamoDbClient dynamoDBClient_;
	private final Region region_;
	private boolean isPrimary_ = false;
	private static final String TEST_TABLE = "Person";

	public DynamoDBConnection(Region region) {
		region_ = region;
	}

	public Region getRegion() {
		return region_;
	}
	
	public String getClientEndpointName()
	{
		try
		{
			List<Endpoint> endpoints = this.getDynamoDbClient().describeEndpoints().endpoints();
			for (Endpoint endpoint : endpoints)
				return endpoint.address();
		}
		catch (Exception ex)
		{
			logger.error("could not get endpoint address due to: " + ex);
		}
		return "unknown";
	}

	public boolean isPrimary() {
		if (dynamoDBClient_ == null)
			this.createConnection();
		return isPrimary_;
	}

	public TableDescription describeTable(DescribeTableRequest request)
	{
		DynamoDbClient connection = this.getDynamoDbClient();
		if (connection != null) 
			return connection.describeTable(request).table();
		
		return null;
	}
	
	public List<String> listTables(ListTablesRequest request)
	{
		DynamoDbClient connection = this.getDynamoDbClient();
		if (connection != null) {
			ListTablesResponse response = connection.listTables(request);
			return response.tableNames();
		}
		
		return new ArrayList<String>();
		
		
	}
	
	public PutItemResponse putItem(PutItemRequest request) {
		DynamoDbClient connection = this.getDynamoDbClient();
		
		PutItemResponse response = null;
		
		if (connection != null) {
			try {
				long start = new Date().getTime();
				response = connection.putItem(request);
				long stop = new Date().getTime();
				logger.info(String.format("PutItem execution time: %d", stop - start));
			} catch (DynamoDbException e) {
				AwsErrorDetails errorDetails = e.awsErrorDetails();

				// access denied means this connection is blocked from being primary
				if (errorDetails.errorCode().startsWith("AccessDenied"))
					isPrimary_ = false;
			}
		}
		else
			response = PutItemResponse.builder().build();
		
		
		return response;
	}

	public GetItemResponse getItem(GetItemRequest request) {
		DynamoDbClient connection = this.getDynamoDbClient();
		GetItemResponse response = null;
		
		if (connection != null) {
			try {
				long start = new Date().getTime();
				response = connection.getItem(request);
				long stop = new Date().getTime();
				logger.info(String.format("GetItem execution time: %d", stop - start));
			} catch (DynamoDbException e) {
				AwsErrorDetails errorDetails = e.awsErrorDetails();

				// access denied means this connection is blocked from being primary
				if (errorDetails.errorCode().startsWith("AccessDenied"))
					isPrimary_ = false;
			}
		}
		else
			response = GetItemResponse.builder().build();
		
		return response;
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

	private DynamoDbClient getDynamoDbClient() {
		if (dynamoDBClient_ == null)
			createConnection();

		return dynamoDBClient_;
	}

	private DynamoDbClient buildNewDynamoClient()
	{
		DynamoDbClientBuilder builder = DynamoDbClient.builder();
		// testing direct access to an endpoint
		// ClientOverrideConfiguration overrideConfig = builder.overrideConfiguration();
		// Builder overrideBuilder = overrideConfig.toBuilder();
		// System.out.println(overrideBuilder);

		// URI uri = new URI("https://dynamodb.us-west-2.amazonaws.com");
		// dynamoDBClient_ = builder.endpointOverride(uri).build();

		return builder.region(region_).build();
	}
	
	private void createConnection() {
		try {
			if (dynamoDBClient_ != null)
			{
				dynamoDBClient_.close();
				dynamoDBClient_ = null;
			}

			
			dynamoDBClient_ = this.buildNewDynamoClient();
			this.setIsPrimary(dynamoDBClient_);
		} catch (Exception ex) {
			logger.error("Error creating DDB Client" + ex);
		}
	}

	boolean validatePrimaryStatus() {
		
		
		logger.info("Checking health of " + this.getFormattedTableName(TEST_TABLE));
		
		GetItemRequest request = this.getConnTestQuery();
		
		try {
			//build new client to eliminate cached IAM permissions
			this.buildNewDynamoClient().getItem(request);
			logger.info(this.getFormattedTableName(TEST_TABLE) + " is available for reads");
			return true;

		} catch (DynamoDbException e) {
			logger.info(this.getFormattedTableName(TEST_TABLE + " is not available for reads"));
			AwsErrorDetails errorDetails = e.awsErrorDetails();

			// access denied means this connection is blocked from being primary
			// any other error, the primary state would be unchanged
			if (errorDetails.errorCode().startsWith("AccessDenied"))
				return false;
			else
				return isPrimary_;
		}
	}

	private String getFormattedTableName(String tableName)
	{
		return String.format("%s/%s", region_.toString(), tableName);
	}
	
	private GetItemRequest getConnTestQuery()
	{
		String key = "last-name";
		String sortKey = "first-name";
		String keyVal = "xyz";
		String sortKeyVal = "Patty";
		

		HashMap<String, AttributeValue> keyToGet = new HashMap<>();
		keyToGet.put(key, AttributeValue.builder().s(keyVal).build());
		keyToGet.put(sortKey, AttributeValue.builder().s(sortKeyVal).build());

		return GetItemRequest.builder().key(keyToGet).tableName(TEST_TABLE).build();
	}
	
	private void setIsPrimary(DynamoDbClient connection) {

		logger.info(String.format("Checking %s for primary", region_.toString()));

		GetItemRequest request = this.getConnTestQuery();
		
		try {
			connection.getItem(request);
			logger.info(this.getFormattedTableName(TEST_TABLE) + " is available for reads");
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
		return String.format("DynamoDBConnection [Region: %s, isPrimary: %s, Endpoint: %s]", region_.toString(), isPrimary_, this.getClientEndpointName());
	}
}

package org.example;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;

public class DynamoDBConnectionManager {

	HashMap<String, DynamoDBConnection> connections_ = new HashMap<>();
	private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnectionManager.class);

	// read from config, defaulted to app settings
	private static final String[] REGIONS = { "us-west-2", "us-east-2" };

	public DynamoDBConnectionManager() {

		for (String region : REGIONS)
			connections_.put(region, this.createConnection(region));

		// start background thread
		connectionPolling();
	}

	private void buildConnections()
	{
		connections_.forEach((region,conn) -> conn.closeConnection());
		for (String region : REGIONS)
			connections_.put(region, this.createConnection(region));
	}
	
	private DynamoDBConnection createConnection(String region) {
		Region r = Region.of(region);
		return new DynamoDBConnection(r);
	}

	public void closeConnections() {
		for (DynamoDBConnection conn : connections_.values())
			conn.closeConnection();
	}

	public DynamoDBConnection getPrimaryConnection() {
		for (DynamoDBConnection conn : connections_.values()) {
			if (conn.isPrimary())
				return conn;
		}

		return null;
	}

	private void connectionPolling() {
		Runnable r = new Runnable() {
			@Override
			public void run() {

				while (true) {
					connections_.forEach((region,conn) -> checkConnection(conn));
					
					// wait for 5 seconds an try again
					try {
						Thread.sleep(5000);
					} catch (InterruptedException ex) {
					}
				}
			}
			
			private void checkConnection(DynamoDBConnection conn)
			{
				logger.debug(String.format("Background thread checking connection %s", conn.toString()));
				boolean isPrimary = conn.isPrimary();
				boolean shouldBePrimary = conn.validatePrimaryStatus();
				
				//if the connection isn't primary, but needs to be
				//then rebuild them
				if (isPrimary != shouldBePrimary)
						buildConnections();
			}
		};
		Thread t = new Thread(r, "Connection Manager");
		t.start();
	}

}

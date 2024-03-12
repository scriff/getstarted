package org.example;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBConnectionManager {

	List<DynamoDBConnection> connections_ = new ArrayList<>();
	private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnectionManager.class);

	public DynamoDBConnectionManager() {
		connections_.add(new DynamoDBConnection(Region.US_EAST_2));
		connections_.add(new DynamoDBConnection(Region.US_WEST_2));
		connectionPolling();
	}

	public void closeConnections() {
		for (DynamoDBConnection conn : connections_)
			conn.closeConnection();
	}

	public DynamoDbClient getPrimaryConnection() {
		for (DynamoDBConnection conn : connections_) {
			if (conn.isPrimary())
				return conn.getConnection();
		}

		// try until we get a primary connection
		try {
			int sleepMillis = 15000;
			logger.debug(String.format("Sleeping for %n millis while waiting for a primary connection", sleepMillis));
			Thread.sleep(sleepMillis);
		} catch (InterruptedException ex) {
		}
		return this.getPrimaryConnection();
	}

	private void connectionPolling() {
		Runnable r = new Runnable() {
			@Override
			public void run() {

				while (true) {
					for (DynamoDBConnection conn : connections_) {

						// check to see if any connections need to be reset
						if (!conn.isPrimary()) {
							logger.debug(String.format("Background thread closing connection %s", conn.toString()));
							conn.closeConnection();
							conn.getConnection();
						}
					}
					// wait for a minute an try again
					try {
						Thread.sleep(60000);
					} catch (InterruptedException ex) {
					}
				}
			}
		};
		Thread t = new Thread(r, "Connection Manager");
		t.start();
	}

}

# App

This project contains a maven application with [AWS Java SDK 2.x](https://github.com/aws/aws-sdk-java-v2) dependencies.

## Prerequisites
- Java 1.8+
- Apache Maven


## Development

- `DynamoDBTest.java` main entry of the application - uses connection manager to get connections, executes table meta-data functions and simple writes (last-name, first-name and timestamp)
- `DynamoDBConnectionManager.java` Simple class that keeps track of the active writer using a sleep to wait for connections to reappear (no exponential backoff, just simple 15 sec polling) and a background thread to proactively fix/detect
- `DynamoDBConnection.java` wraps the aws dynamo connection, primarily to keep track of region and if the connection should be serving as a primary (which is done via simply trying to write to the stable)

#### Building the project
```
mvn clean package
```
#### Executing the project
- note that you will need AWS resources set up and local configurations with the correct access keys and secrets
```
mvn clean package
```

```
mvn clean compile exec:java
```


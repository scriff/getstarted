package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class Handler {
	private static final Logger logger = LoggerFactory.getLogger(Handler.class);

	private final S3Client s3Client_;

	public Handler() {
		logger.info("Handler instantiated");
		s3Client_ = DependencyFactory.s3Client();
		logger.info("Handler client built");

	}

	public void sendRequest() {
		String bucket = "bucket" + System.currentTimeMillis();
		String key = "key";

		createBucket(s3Client_, bucket);

		logger.info("Uploading object...");

		s3Client_.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
				RequestBody.fromString("Testing with the {sdk-java}"));

		logger.info("Upload complete");
		logger.info("\n");

		cleanUp(s3Client_, bucket, key);

		logger.info("Closing the connection to {S3}");
		s3Client_.close();
		logger.info("Connection closed");
		logger.info("Exiting...");
	}

	public static void createBucket(S3Client s3Client, String bucketName) {
		try {
			s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
			logger.info("Creating bucket: " + bucketName);
			s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());
			logger.info(bucketName + " is ready.");
		} catch (S3Exception e) {
			logger.error(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}

	public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
		logger.info("Cleaning up...");
		try {
			logger.info("Deleting object: " + keyName);
			DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName)
					.build();
			s3Client.deleteObject(deleteObjectRequest);
			logger.info(keyName + " has been deleted.");
			logger.info("Deleting bucket: " + bucketName);
			DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
			s3Client.deleteBucket(deleteBucketRequest);
			logger.info("Deleted bucket: " + bucketName);
		} catch (S3Exception e) {
			logger.error(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		logger.info("Cleanup complete");
	}
}

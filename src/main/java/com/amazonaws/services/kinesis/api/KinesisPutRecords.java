package com.amazonaws.services.kinesis.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

public class KinesisPutRecords {
	public static void main(String[] args) {
		AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient().withEndpoint("https://kinesis.eu-west-1.amazonaws.com").
				withRegion(Regions.EU_WEST_1);
		PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
		System.out.println("Am here....");
		putRecordsRequest.setStreamName("New_Lambda_Stream");
		List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>(); 
		for (int i = 239000; i <= 239499; i++) {
		    PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
		    putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).concat(",").getBytes()));
		    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
		    putRecordsRequestEntryList.add(putRecordsRequestEntry); 
		}

		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		System.out.println("My Put Records are ready....");
		PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
		System.out.println("Put Result" + putRecordsResult);
	}
}

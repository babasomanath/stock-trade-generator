/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.application.producer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.application.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.application.stocktrades.writer.StockTradeGenerator;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
/**
 *
 */
public class Generator {

    private static final Logger log = LoggerFactory.getLogger(Generator.class);
    
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(3);
    
    /**
     * Timestamp we'll attach to every record
     */
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());

    public static final String STREAM_PARAM = "stream-name";

    public static final String BACKPRESSURE_SIZE = "backpressure-size";

    public static final String BACKPRESSURE_DELAY = "backpressure-delay";

    public static final String THROUGHPUT_SWITCH = "throughput-switch";

    private static String streamName;

    volatile private static int backpressureSize;

    volatile private static int backpressureDelay;

    private static int throughputSwitch = 0;

    private static int throughputFlag = 0;

    //private static StockTrade trade;

    //private static long tradeId;
    //private static String tradeString;

     // The monotonically increasing sequence number we will put in the data of each record
     final private static AtomicLong sequenceNumber = new AtomicLong(0);

        // The number of records that have finished (either successfully put, or failed)
     final private static AtomicLong completed = new AtomicLong(0);
     final private long startTimeMain = System.nanoTime();



    private static KinesisProducer producer;

    /**
     * Here'll walk through some of the config options and create an instance of
     * KinesisProducer, which will be used to put records.
     * 
     * @return KinesisProducer instance used to put records.
     */
    public static KinesisProducer getKinesisProducer() {

         KinesisProducerConfiguration config =
             KinesisProducerConfiguration.fromPropertiesFile("config.properties");
        

        System.out.println("AggregationCount : " + config.getAggregationMaxCount());
        System.out.println("AggregationMaxSize : " + config.getAggregationMaxSize());
        System.out.println("CollectionMaxCount : " + config.getCollectionMaxCount());
        System.out.println("CollectionMaxSize : " + config.getCollectionMaxSize());
        System.out.println("ConnectTimeout : " + config.getConnectTimeout());
        System.out.println("RateLimit : " + config.getRateLimit());
        System.out.println("RecordMaxBufferedTime : " + config.getRecordMaxBufferedTime());
        System.out.println("RecordTtl : " + config.getRecordTtl());
        System.out.println("RequestTimeout : " + config.getRequestTimeout());
        System.out.println("MaxConnections : " + config.getMaxConnections());
        
        // You can pass credentials programmatically through the configuration,
        // similar to the AWS SDK. DefaultAWSCredentialsProviderChain is used
        // by default, so this configuration can be omitted if that is all
        // that is needed.
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        

        KinesisProducer producer = new KinesisProducer(config);
        
        return producer;
    }
    
    public static void main(String[] args) throws Exception {
        producer = getKinesisProducer();

        if (System.getProperty(STREAM_PARAM) == null) {
            log.error("You must provide a Stream Name");
            System.exit(1);
        } else {
            streamName = System.getProperty(STREAM_PARAM);
        }


        if (System.getProperty(THROUGHPUT_SWITCH) != null ) {
            throughputSwitch = Integer.parseInt(System.getProperty(THROUGHPUT_SWITCH));
        }

        if (System.getProperty(BACKPRESSURE_SIZE) == null) {
            if (throughputSwitch == 0) {
                log.error("You must provide a backpressure outstanding size");
                System.exit(1);
            }
        } else {
            backpressureSize = Integer.parseInt(System.getProperty(BACKPRESSURE_SIZE));
        }

        if (System.getProperty(BACKPRESSURE_DELAY) == null ) {
            if (throughputSwitch == 0) {
                log.error("You must provide a backpressure delay time (in milliseconds)");
                System.exit(1);
            }
        } else {
            backpressureDelay = Integer.parseInt(System.getProperty(BACKPRESSURE_DELAY));
        }


        


        // This gives us progress updates
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long put = sequenceNumber.get();

                long done = completed.get();

                log.info(String.format(
                        "Put %d of %d completed so far",
                        done, put));
            }
         }, 1, 1, TimeUnit.SECONDS);

        // This gives us progress updates
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (throughputSwitch == 1) {
                    if (throughputFlag == 0) {
                        backpressureSize = 300000;
                        backpressureDelay = 100;
                        throughputFlag = 1;
                    } else if (throughputFlag == 1) {
                        backpressureSize = 20000;
                        backpressureDelay = 1000;
                        throughputFlag = 0;
                    }
                    // This is just a dummy. Currently I have only throughputSwitch=1 tested. throughputFlag=0 can max out 10MBps. throughputFlag=1 max is 5MBps
                } else if (throughputSwitch == 2) {
                    if (throughputFlag == 0) {
                        backpressureSize = 300000;
                        backpressureDelay = 10;
                        throughputFlag = 1;
                    } else if (throughputFlag == 1) {
                        backpressureSize = 20000;
                        backpressureDelay = 100;
                        throughputFlag = 0;
                    }
                }

                log.info(String.format(
                        "============= backpressureSize: %d. backpressureDelay: %d ===============",
                        backpressureSize, backpressureDelay));

            }
        }, 0, 300, TimeUnit.SECONDS);

        while(true) {
            sequenceNumber.getAndIncrement();

            try {
	        	putOneRecordMethod(StockTradeGenerator.getRandomTrade());
            } catch (Exception e) {
                log.error("Error running task", e);
                System.exit(1);
            }
        }
    }

    private static void putOneRecordMethod(final StockTrade trade) throws InterruptedException {
    		log.info("The Trading data :  "+trade.toString());
            byte[] bytes = trade.toJsonAsBytes();
                // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
                if (bytes == null) {
                    log.info("Could not get JSON bytes for stock trade");
                    return;
                }

		    FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
                    @Override
                    public void onFailure(Throwable t) {


                        if (t instanceof UserRecordFailedException) {
                            Attempt last = Iterables.getLast(
                                    ((UserRecordFailedException) t).getResult().getAttempts());
                            log.info(String.format(
                                    "Record failed to put - %s : %s : %s" ,
                                    trade.toString(), last.getErrorCode(), last.getErrorMessage()));
                            //log.error("Exception during put", t);

                        }
                    }

                    @Override
                    public void onSuccess(UserRecordResult result) {
                        completed.getAndIncrement();
                        log.info(String.format(
                                "Succesfully put record, "
                                        + "payload=%s", trade.toString()
                                        + "  sequenceNumber=%s  shardId=%s  took %d attempts",
                                        result.getSequenceNumber(),result.getShardId(),result.getAttempts().size()));
                    }
                };


            while (producer.getOutstandingRecordsCount() > backpressureSize) {
                Thread.sleep(backpressureDelay);
            }

            ListenableFuture<UserRecordResult> f =
                producer.addUserRecord(streamName, TIMESTAMP, Utils.randomExplicitHashKey(), ByteBuffer.wrap(bytes));
                Futures.addCallback(f, callback);
    }
}

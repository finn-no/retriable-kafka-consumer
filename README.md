[![Known Vulnerabilities](https://snyk.io/test/github/finn-no/retriable-kafka-consumer/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/finn-no/retriable-kafka-consumer?targetFile=pom.xml)

[![Build Status](https://travis-ci.com/finn-no/retriable-kafka-consumer.svg?branch=master)](https://travis-ci.com/finn-no/retriable-kafka-consumer)

# Retriable Kafka Consumer

`retriable-kafka-consumer` is a library for retriable event-processing using kafka. 
If processing of a kafka-record throws an unhandled exception, the record is re-sent to a retry-topic. 
The record will then be retried until it is successfully processed without exceptions or the `retryPeriod` is exceeded.



# Usage

## Limitations

Since this library re-posts your messages to a new topic, the order of the messages is not preserved. So if you depend on in-order-processing, this library is not for you.

## Dependency

From maven central:

        implementation 'no.finn.retriable-kafka-consumer:retriable-kafka-consumer:1.54'   

or 

	<dependency>
	  <groupId>no.finn.retriable-kafka-consumer</groupId>
	  <artifactId>retriable-kafka-consumer</artifactId>
	  <version>1.54</version>
	</dependency>


## Requirements 

* In your kafka-config, turn off auto-commit. The library will handle commits.

* You need to implement a `KafkaClientFactory`:

        public interface KafkaClientFactory<K, V> {
    
            Consumer<K, V> consumer();
    
            Producer<K, V> producer();
    
            String groupId();
      }

    

## Minimal example

    pool = new ReliablePoolBuilder<>(eventClientFactory)
                    .topics(List.of(TOPIC))
                    .processingFunction(record -> process(record, adStore, insightEnqueuer))
                    .build();
                    
    pool.monitor.start();

## All options included  

    // build a consumer-pool 
    ReliableKafkaConsumerPool<String,String> pool = new ReliablePoolBuilder<>(eventClientFactory)
                .topics(List.of(TOPIC, TOPIC2)) // list of topics to subscribe to, will generate retry topics
                .poolCount(5) // optional: number of threads consuming from kafka 
                .processingFunction(record -> process(record)) // required : a function that processes records
                .logHandler(new DefaultLogHandler()) // optional : logging implementation (a default will be provided if omitted)
                .expiredHandler(t -> System.out.println("t = " + t)) // optional: what to do with expired messages
                .retryPeriodMillis(24 * 60 * 60 * 1000) // optional: how long a message should be retried before given up on
                .retryThrottleMillis(5_000) // optional: specify how fast a message should be retried
                .build();
                
    pool.monitor.start(); // start the pool
    
    
    // optional: check if all consumer-threads are alive, recommended to check these every minute or so
    pool.monitor.monitor()

## Explicitly set retry topics  

    // build a consumer-pool 
    ReliableKafkaConsumerPool<String,String> pool = new ReliablePoolBuilder<>(eventClientFactory)
                .topicsRetryTopics(Collections.singletonMap(TOPIC, RETRY_TOPIC)) //explicitly set retry topic.
                .poolCount(5) // optional: number of threads consuming from kafka 
                .processingFunction(record -> process(record)) // required : a function that processes records
                .retryPeriodMillis(24 * 60 * 60 * 1000) // optional: how long a message should be retried before given up on
                .retryThrottleMillis(5_000) // optional: specify how fast a message should be retried
                .build();
                
    pool.monitor.start(); // start the pool
    
    // optional: check if all consumer-threads are alive, recommended to check these every minute or so
    pool.monitor.monitor()

# Detailed description 

This library is inspired by an article written by [Über Engineering](https://eng.uber.com/reliable-reprocessing/), but takes a slightly different approach.
Instead of using _n_ retry-topics and a dead-letter-queue, we use 1 retry queue and timestamps/counters in the record header to check if a record is expired. 

The retry-topic will be named "retry-<groupid>-<original-topic>", and the pool will consume messages from this topic in the same way as the original topic. 
Consumer-threads are made restartable, meaning if any of the threads die for some reason they can be restarted by the monitor. 

Reprocessing is controlled by headers on the kafka-record to keep track of how many times a record has been processed and a timestamp for the initial retry-attempt. 

## Metrics

Metric counters are exposed via prometheus. 

* expired_events, number of events that has been reprocessed until `retryPeriod` is exceeded. 
* failed_events, number of events that has failed and will be reprocessed
* processed_successfully_events, number or successful events processed

These are all in the namespace of the application, the system-property or system-environment variable "ARTIFACT_NAME". If "ARTIFACT_NAME" is not specified, the value "unknown app" will be used instead.


# Development

* Use java 8 (e.g. 8.0.222-amzn)

## Release
1. Update pom version to final version, commit and push
2. Upload to nexus:
	
MacOs only:
  
	$ export GPG_TTY=$(tty)     
	
All platforms:

	$ mvn clean deploy -Psign  
	$ mvn nexus-staging:release

3. Tag current revision with "v<VERSION-NUMBER>"
4. Push tags
5. Increment current version to -SNAPSHOT

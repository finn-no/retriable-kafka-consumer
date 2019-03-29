# Retriable Kafka Consumer

`retriable-kafka-consumer` is a library for retriable event-processing using kafka. 
If processing of a kafka-record throws an unhandled exception, the record is re-sent to a retry-topic. 
The record will then be retried until it is successfully processed without exceptions or the `retryPeriod` is exceeded.

# Usage

## Requirements 

* In your kafka-config, turn of auto-commit. 
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
                .topics(List.of(TOPIC, TOPIC2)) // list of topics to subscribe to
                .poolCount(5) // optional: number of threads consuming from kafka 
                .afterProcess(consumer -> commit(consumer)) // optional: after a poll, do something with consumer
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

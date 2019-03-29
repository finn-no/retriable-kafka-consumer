# Retriable Kafka Consumer

`retriable-kafka-consumer` is a library for retriable event-processing using kafka. 
If processing of a kafka-record throws an unhandled exception, the record is re-sent to a retry-topic. 
The record will then be retried until it is successfully processed without exceptions or the `retryPeriod` is exceeded.

# Usage

## Minimal example

    pool = new ReliablePoolBuilder<>(eventClientFactory)
                    .topics(List.of(TOPIC))
                    .processingFunction(record -> process(record, adStore, insightEnqueuer))
                    .build();
                    
    pool.start();

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


# Detailed description goes here

This library is inspired by an article written by [Über Engeneering](https://eng.uber.com/reliable-reprocessing/), but takes a slightly different approach.
Instead of using _n_ retry-topics and a dead-letter-queue, we use 1 retry queue and timestamps in the record headers to check if a record is expired. 

The retry-topic is TODODO

* poolcount
* retry-topics
* commit
* default values
* inspiration, link to über article
* restartable consumers
* prometheus metrics, version export
* headers for counting and timing (kafka 2.0?)



# IngestRedisStreams

This is a simple application that demonstrates how Redis Streams work. The producer side of the application gathers new Twitter messages and writes them to a Redis Stream data structure. The consumer reads the data, deciphers the JSON data of the message, selects Twitter handles that have more than 10,000 followers as influencers, and maintains a catalog of influencers in Redis.

## Setup

1. Redis: Install Redis 5.0. Redis Streams is a new data structure that's available in version 5.0 and above. For more information, visit https://redis.io.

2. Java: Install JDK verion 1.8 or above.

3. Lettuce: Download Lettuce 5.1.x or above, and all the libraries required by it. Make sure all the libraries are in your classpath. For more information, visit https://lettuce.io/.

4. PubNub: Download PubNub Java SDK and libraries into your classpath. For more information visit, https://www.pubnub.com/docs/java-se-java/pubnub-java-sdk.

5. Update the following Java programs:
   a. LettuceConnection.java: Change the Redis connection URI to connect to your Redis server
   b. InitializeConsumerGroup.java: If you don't want the default names for STREAM_ID and GROUP_ID, change them.
   c. TwitterIngestStream.java: Create a PubNub key for yourself and change it.
   
## Compiling

Make sure all the libraries are in your classpath and compile the code with command, javac ./src/*.

## Execution

1. InitializeConsumerGroup - run java com.redislabs.ingest.streams.InitializeConsumerGroup
2. TwitterIngestStream - run java com.redislabs.ingest.streams.TwitterIngestStream
3. InfluencerCollectorMain - run java com.redislabs.ingest.streams.InfluencerCollectorMain


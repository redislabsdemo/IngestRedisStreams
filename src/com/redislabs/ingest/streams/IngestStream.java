package com.redislabs.ingest.streams;

import io.lettuce.core.api.sync.RedisCommands;

/**
 * IngestStream class allows you to write data to a Redis Stream. 
 * You can run this class to test whether you have the right version 
 * of Redis that supports Redis Stream. Typically you extend IngestStream
 * to provide your own implementation. For example, TwitterIngestStream
 * extends IngestStream.   
 *
 */
public class IngestStream{
	
	protected String streamId = null;

	protected LettuceConnection connection = null;
	protected RedisCommands<String, String> commands = null;

	// Hide the constructor and force external objects to instantiate
	// via the factory method
	protected IngestStream() {
		
	}
	
	// Factory method to instantiate the object. This method instantiates the object
	// and creates the connection to the Redis database
	public synchronized static IngestStream getInstance(String streamId) throws Exception{
		IngestStream ingestStream = new IngestStream();
		ingestStream.streamId = streamId;
		ingestStream.init();
		return ingestStream;
	}
	
	// Initializes the Lettuce library
	protected void init() throws Exception{
		connection = LettuceConnection.getInstance();
		commands =  connection.getRedisCommands();		
	}
	
	// Adds the key-value pair as the stream data
	// In Redis Stream, you could pass multiple key-value pairs
	// for a single data object. For simplicity, we will save one 
	// object per line.
	public void add(String key, String message) throws Exception{
		commands.xadd(streamId, key, message);
	}
	
	
	// Use this for testing only
	public static void main(String[] args) throws Exception{
		IngestStream ingest = IngestStream.getInstance("mystream");

		for(int i=20; i<30; i++) {
			ingest.add("n"+i, "v"+i);
		}
	}
	
}
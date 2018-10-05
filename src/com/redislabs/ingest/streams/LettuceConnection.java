package com.redislabs.ingest.streams;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;


/**
 * This is a wrapper class around Lettuce library. 
 * 
 */
public class LettuceConnection{
	
	private RedisClient client = null;
	private StatefulRedisConnection<String, String> connection = null;
	
	private LettuceConnection() {
		
	}
	
	public synchronized static LettuceConnection getInstance() throws Exception{
		LettuceConnection lettuceConnection = new LettuceConnection();
		lettuceConnection.init();
		return lettuceConnection;
	}
		
	private void init() throws Exception{
		try {
			// Make sure to change the URL if it is different in your case
			client = RedisClient.create("redis://127.0.0.1:6379");
			connection = client.connect();									
		}catch(Exception e) {
			e.printStackTrace();
			throw e;
		}	
	}
	
	public StatefulRedisConnection<String, String> getRedisConnection() throws Exception{
		if(connection == null) {
			this.init();
		}
		return connection;
	}
	
	public RedisCommands<String, String> getRedisCommands() throws Exception{
		if(connection == null) {
			this.init();
		}
		
		return connection.sync();
	}
	
	public void close() throws Exception{
		if(connection != null) {
			connection.close();				
		}
		
		if(client != null) {
			client.shutdown();
		}
	}
	
}
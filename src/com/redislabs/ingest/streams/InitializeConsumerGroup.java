package com.redislabs.ingest.streams;

import java.util.HashMap;

import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis Stream, in general, doesn't require initialization. In our demo, 
 * we show how you could use a consumer group to read the data. Redis 
 * does not allow you to create a consumer group with an empty Redis Stream.
 * Therefore, we add a line of dummy data to the stream and create a consumer
 * group.
 *
 * IMPORTANT: Run this program only once before running other programs.
 * 
 */

public class InitializeConsumerGroup{
	
	public static final String STREAM_ID = "twitterstream";
	public static final String GROUP_ID = "influencer";
	
	
	private static LettuceConnection connection = null;
	private static RedisCommands<String, String> commands = null;
			
	public static void main(String[] args) throws Exception{

		connection = LettuceConnection.getInstance();;
		commands = connection.getRedisCommands();
		
		initStream();
		initGroup();
		
	}
	
	private static void initStream() throws Exception{
		String type = commands.type(STREAM_ID);
		
		if(type != null && !type.equals("stream")) {
			commands.del(STREAM_ID);
			addRawData();
		}
		
		if(type == null){
			addRawData();
		}
	}
	
	private static void addRawData() throws Exception{
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("start", "stream");
		commands.xadd(STREAM_ID, map);		
	}
	
	private static void initGroup() {
		try {
			commands.xgroupCreate(STREAM_ID, GROUP_ID, "0");
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
}
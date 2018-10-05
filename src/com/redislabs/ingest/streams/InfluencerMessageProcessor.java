package com.redislabs.ingest.streams;

import java.util.HashMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.lettuce.core.api.sync.RedisCommands;


/**
 * This is a message processor object that reads the twitter stream,
 * collects influencer information, and stores it back in Redis. 
 * 
 */
public class InfluencerMessageProcessor implements MessageProcessor{
	
	LettuceConnection connection = null;
	RedisCommands<String, String> commands = null;

	// Factory method
	public synchronized static InfluencerMessageProcessor getInstance() throws Exception{
		InfluencerMessageProcessor processor = new InfluencerMessageProcessor();
		processor.init();
		return processor;
	}
	
	// Suppress instantiation outside the factory method
	private InfluencerMessageProcessor() {
		
	}
	
	// Initialize Redis connections
	private void init() throws Exception{
		connection = LettuceConnection.getInstance();
		commands =  connection.getRedisCommands();				
	}
	
	
	@Override
	public void processMessage(String message) throws Exception {
		try {
			JsonParser jsonParser = new JsonParser();
			JsonElement jsonElement = jsonParser.parse(message);
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			JsonObject userObject = jsonObject.get("user").getAsJsonObject();

			JsonElement followerCountElm = userObject.get("followers_count");
			
			// 10,000 is just an arbitrary number. We are marking any handle with 
			// more than 10,000 followers as an influencer.
			if (followerCountElm != null && followerCountElm.getAsDouble() > 10000) {
				String name = userObject.get("name").getAsString();
				String screenName = userObject.get("screen_name").getAsString();
				int followerCount = userObject.get("followers_count").getAsInt();
				int friendCount = userObject.get("friends_count").getAsInt();

				HashMap<String, String> map = new HashMap<String, String>();
				map.put("name", name);
				map.put("screen_name", screenName);
				if (userObject.get("location") != null) {
					map.put("location", userObject.get("location").getAsString());
				}
				map.put("followers_count", Integer.toString(followerCount));
				map.put("friendCount", Integer.toString(friendCount));

				
				// Lettuce commands that store influencer information in Redis
				commands.zadd("influencers", followerCount, screenName);
				commands.hmset("influencer:" + screenName, map);
				
				// Remove this line if you don't want to read the data
				System.out.println(userObject.get("screen_name").getAsString() + "| Followers:"
						+ userObject.get("followers_count").getAsString());
			}

		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}
	}
}
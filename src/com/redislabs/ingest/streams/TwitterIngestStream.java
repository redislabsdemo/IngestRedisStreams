package com.redislabs.ingest.streams;

import java.util.Arrays;

import com.google.gson.JsonObject;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

/**
 * This is the main producer class. When you run this program, it collects
 * Twitter data from the PubNub channel and adds them to the Redis Stream 
 * 
 */
public class TwitterIngestStream extends IngestStream{

	// Follow instructions on PubNub to get your own key
	final static String SUB_KEY_TWITTER = "sub-c-1111111-2222-2222-3333-444444444444"; // Change the key
	final static String CHANNEL_TWITTER = "pubnub-twitter";
	
	// Factory method
	public synchronized static TwitterIngestStream getInstance(String streamId) throws Exception{
		TwitterIngestStream ingestStream = new TwitterIngestStream();
		ingestStream.streamId = streamId;
		ingestStream.init();
		return ingestStream;
	}
	
	// Making the constructor private to force creating new objects through the factory method
	private TwitterIngestStream() {
		
	}
	
	// The main method
	public static void main(String[] args) throws Exception{
		
		TwitterIngestStream twitterIngestStream = TwitterIngestStream.getInstance(InitializeConsumerGroup.STREAM_ID);
		twitterIngestStream.start();
	}
	
	// Following PubNub's example 
	public void start() throws Exception{
			TwitterIngestStream ingestStream = this;
			PNConfiguration pnConfig = new PNConfiguration();
			pnConfig.setSubscribeKey(SUB_KEY_TWITTER);
			pnConfig.setSecure(false);
			
			PubNub pubnub = new PubNub(pnConfig);
			
			pubnub.subscribe().channels(Arrays.asList(CHANNEL_TWITTER)).execute();
			
			
			// PubNub event callback
			SubscribeCallback subscribeCallback = new SubscribeCallback() {
			    @Override
			    public void status(PubNub pubnub, PNStatus status) {
			        if (status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory) {
			            // internet got lost, do some magic and call reconnect when ready
			            pubnub.reconnect();
			        } else if (status.getCategory() == PNStatusCategory.PNTimeoutCategory) {
			            // do some magic and call reconnect when ready
			            pubnub.reconnect();
			        } else {
			            System.out.println(status.toString());
			        }
			    }
			 
			    // Receive the message and add to the RedisStream
			    @Override
			    public void message(PubNub pubnub, PNMessageResult message) {
			    	try{
			    		JsonObject json = message.getMessage().getAsJsonObject();
			    		
			    		// Delete this line if you don't need this log
			    		System.out.println(json.toString());
			
			    		// Each line or data entry of a Redis Stream is a collection of key-value pairs
			    		// For simplicity, we store only one key-value pair per line. "tweet" is the key
			    		// for each line. Note, that it's not the entry id, because Redis Streams 
			    		// autogenerates the entry id.
			    		//
			    		// Example of a Redis Stream: 
			    		// twitterstream
			    		//		1837847490983-0 tweet {....}
			    		//		1837847490984-0 tweet {....}
			    		//		1837847490986-0 tweet {....}
			    		//		1837847490987-0 tweet {....}
			    		ingestStream.add("tweet", json.toString());
			    	}catch(Exception e){
			    		e.printStackTrace();
			    	}
			    	
			    	
			    }
			 
			    @Override
			    public void presence(PubNub pubnub, PNPresenceEventResult presence) {
			    }
			};
			 
			// Add callback as a listener (PubNub code) 
			pubnub.addListener(subscribeCallback);	
			
		}
		
						
	

}
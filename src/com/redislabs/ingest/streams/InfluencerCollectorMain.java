package com.redislabs.ingest.streams;

/**
 * This is the main consumer class. It does the following:
 * a. Initiates a StreamConsumer object to read data from the Redis Stream named
 *    "twitterstream", consumer group called "influencer" and consumer "a"
 * b. Starts a StreamConsumer in a separate thread
 * c. Reads only new messages
 *  
 */
public class InfluencerCollectorMain{
	
	public static void main(String[] args) throws Exception{
		StreamConsumer influencerStreamGroupReader  = null;
		
		try {
			InfluencerMessageProcessor imProcessor = InfluencerMessageProcessor.getInstance();
			/*
			 * Redis Stream name = twitterstream (InitializeConsumerGroup.STREAM_ID)
			 * Consumer group = influencer (InitializeConsumerGroup.GROUP_ID)
			 * Consumer = a
			 * Message processor = InfluncerMessageProccessor object 
			 */
			influencerStreamGroupReader = new StreamConsumer(InitializeConsumerGroup.STREAM_ID,InitializeConsumerGroup.GROUP_ID,"a", 
					StreamConsumer.READ_NEW, imProcessor);
			Thread t = new Thread(influencerStreamGroupReader);
			t.start();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
package com.redislabs.ingest.streams;

/**
 * MessageProcessor type declares a method, processMessage. This 
 * data type is passed on to the StreamConsumer object. StreamConsumer
 * calls the processMessage method for every data item in the stream.
 * You should provide your own implementation of how to process the data.
 * 
 * In our example, InfluencerMessageProcessor implements MessageProcessor
 */
public interface MessageProcessor{
	
	public void processMessage(String message) throws Exception;
	
}
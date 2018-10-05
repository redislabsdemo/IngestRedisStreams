package com.redislabs.ingest.streams;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;


/**
 * This is the consumer class that reads the data from RedisStream. 
 * In our example, InfluencerCollectorMain initiates StreamConsumer
 * and starts it as a separate thread. The thread waits for a new 
 * message via a blocking call. It expires every 5 seconds and 
 * rechecks for a new message. 
 * 
 */
public class StreamConsumer implements Runnable{
	
	public static final String READ_FROM_START = "0";
	public static final String READ_NEW = "$";
	
	String streamId = null;
	String groupId = null;
	String consumerId = null;
	String readFrom = READ_NEW;
	MessageProcessor messageProcessor = null;
	
	LettuceConnection connection = null;
	RedisCommands<String, String> commands = null;
	
	public StreamConsumer(String streamId, String groupId, String consumerId, 
			String readFrom, MessageProcessor messageProcessor) throws Exception{
		this.streamId = streamId;
		this.groupId = groupId;
		this.consumerId = consumerId;
		this.readFrom = readFrom;
		this.messageProcessor = messageProcessor;
		
		connection = LettuceConnection.getInstance();
		commands =  connection.getRedisCommands();

	}
	
	public void readStream() throws Exception{	
	
		boolean reachedEndOfTheStream = false;
		while(!reachedEndOfTheStream) {
			List<StreamMessage<String, String>> msgList = getNextMessageList();
			
			if(msgList.size()==0) {
				reachedEndOfTheStream = true;
			}else {
				processMessageList(msgList);
			}
		}
		
	}
	
	// Non-blocking call
	private List<StreamMessage<String, String>> getNextMessageList() throws Exception{
		return commands.xreadgroup(
					Consumer.from(groupId, consumerId),
					XReadArgs.Builder.count(1),
					XReadArgs.StreamOffset.from(streamId, "0"));
	}
	
	
	// Blocking call; blocks for 5 seconds
	private List<StreamMessage<String, String>> getNextMessageListBlocking() throws Exception{
		return commands.xreadgroup(
					Consumer.from(groupId, consumerId),
					XReadArgs.Builder.count(1).block(Duration.ofSeconds(5)),
					XReadArgs.StreamOffset.lastConsumed(streamId));

	}
	
	// processes the message and reports back to Redis Stream with XACK
	private void processMessageList(List<StreamMessage<String, String>> msgList) {
		
		if(msgList.size()> 0) {
			Iterator itr = msgList.iterator();
			while(itr.hasNext()) {
				StreamMessage<String, String> message = 
						(StreamMessage<String, String>)itr.next();
				
				Map<String, String> body = message.getBody();	
				String msgId = message.getId();
				Iterator keyItr = body.keySet().iterator();				
				while(keyItr.hasNext()) {
					String key = (String)keyItr.next();
					String value = (String)body.get(key);
					try {
						messageProcessor.processMessage(value);
						commands.xack(streamId, groupId, msgId);
					}catch(Exception e) {
						System.out.println(e.getMessage());
					}
				}
				
			}
		}		
	}
	
	private boolean stopThread = false;
	
	public void close() throws Exception{
		stopThread = true;
		if(connection != null) {
			connection.close();
		}
	}
	
	// This is helpful during the startup. It helps the consumer
	// to catch up with the messages that it has not read so far
	private boolean processPendingMessages() throws Exception{
		
		boolean pendingMessages = true;
		
		List<StreamMessage<String, String>> msgList = getNextMessageList();
		
		if(msgList.size()!=0) {
			processMessageList(msgList);
		}else {
			System.out.println("Done processing pending messages");
			pendingMessages = false;
		}
		
		return pendingMessages;
	}
	
	// Read messages at runtime
	private void processOngoingMessages() throws Exception{
		List<StreamMessage<String, String>> msgList = getNextMessageListBlocking();
		
		if(msgList.size()!=0) {
			processMessageList(msgList);
		}else {
			System.out.println("******Group: "+groupId+" waiting. No new message*****");						
		}		
	}
	
	// Thread function
	public void run() {
		try {
			boolean pendingMessages = true;
			while(pendingMessages) {
				pendingMessages = processPendingMessages();
			}
			
			while(!stopThread) {
				processOngoingMessages();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}

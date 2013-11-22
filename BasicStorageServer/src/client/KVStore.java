package client;



import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

import common.messages.MessageProcessing;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	 
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
 	
 	private String address ;
 	private int port;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	
	
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		
		this.address=address;
		this.port=port;
		listeners = new HashSet<ClientSocketListener>();
		
	}
	
	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		setRunning(true);
		byte [] serverResponse = this.KVrecievemessage();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(new String(serverResponse).trim());
		}
		logger.info("Connection established");
		
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		
		try {
			setRunning(false);
			logger.info("closing the connection ...");
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
				logger.info("connection closed!");
			}
			
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
		
	}
	

	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}
	
	public void KVsendmessage(byte [] message) throws IOException {
		message = this.appendChars(message);
		output.write(message, 0, message.length);
		output.flush();
	}
	
	
	private byte[] KVrecievemessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		

		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			

				bufferBytes[index] = read;
				index++;

			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
	
		return msgBytes;
    }

	@Override
	public KVMessage put(String key, String value) throws Exception {

		MessageProcessing messageToSend = new MessageProcessing();
		messageToSend.setKey(key);
		messageToSend.setValue(value);
		messageToSend.setStatus(StatusType.PUT);
		this.KVsendmessage(messageToSend.messageEncoding());
		MessageProcessing messageToReceive = new MessageProcessing();
		messageToReceive.messageDecoding(this.KVrecievemessage());
		return messageToReceive;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		MessageProcessing messageToSend = new MessageProcessing();
		messageToSend.setKey(key);
		messageToSend.setValue("");
		messageToSend.setStatus(StatusType.GET);
		this.KVsendmessage(messageToSend.messageEncoding());
		MessageProcessing messageToReceive = new MessageProcessing();
		messageToReceive.messageDecoding(this.KVrecievemessage());
		
		return messageToReceive;
	}
	
	private byte[] appendChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}


	
}

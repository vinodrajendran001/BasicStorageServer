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
		byte [] serverResponse = this.receiveMessage();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(new String(serverResponse).trim());
		}
		logger.info("Connection established");
		
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
logger.info("try to close connection ...");
		
		try {
			tearDownConnection();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
		
	}
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
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
	
	public void SendMessage(byte [] message) throws IOException {
		message = this.addCtrChars(message);
		output.write(message, 0, message.length);
		output.flush();
	}
	
	
	private byte[] receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
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
			
			/* only read valid characters, i.e. letters and numbers */
//			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			//}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
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
		
		/* build final String */
		/*TextMessage msg = new TextMessage(msgBytes);
		logger.info("Receive message:\t '" + msg.getMsg() + "'");*/
		return msgBytes;
    }

	@Override
	public KVMessage put(String key, String value) throws Exception {

		MessageProcessing messageToSend = new MessageProcessing();
		messageToSend.setKey(key);
		messageToSend.setValue(value);
		messageToSend.setStatus(StatusType.PUT);
		this.SendMessage(messageToSend.messageEncoding());
		MessageProcessing messageToReceive = new MessageProcessing();
		messageToReceive.messageDecoding(this.receiveMessage());
		return messageToReceive;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		MessageProcessing messageToSend = new MessageProcessing();
		messageToSend.setKey(key);
		messageToSend.setValue("");
		messageToSend.setStatus(StatusType.GET);
		this.SendMessage(messageToSend.messageEncoding());
		MessageProcessing messageToReceive = new MessageProcessing();
		messageToReceive.messageDecoding(this.receiveMessage());
		
		return messageToReceive;
	}
	
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}


	
}

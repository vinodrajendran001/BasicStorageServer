package server;

import java.io.IOException;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.*;
import common.messages.KVMessage.StatusType;

import java.util.*;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */

public class ClientConnection implements Runnable {
	
private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	
	List <Data> p ;
	
	private Database db = new Database();
	private HandleKVMessage hk = new HandleKVMessage();
	
	//client request to the server
	private TextMessage request = null;
	private String req_put = null;
	private String req_get = null;
	
	//server response to the client
	private byte[] response = new byte[BUFFER_SIZE];
	
	private String command;
	private String key;
	private String value;
	
	boolean validity = true;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket,List <Data> p) {
		this.p = p ;
		this.clientSocket = clientSocket;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			sendMessage(new TextMessage(
					"Connection to MSRG KV Server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort()));
			
			while(isOpen) {
				try {
					/*
					 * At this point the server receives a get or a put request from the client
					 * as a result he must parse the client's message and then perform the requested operation
					 * Then he sends back his response to the client depending on the outcome.
					 * */
					
					
					request = receiveMessage();
					
					//unmarshall here
					hk.decodeKVMessage(request.getMsgBytes());
					//load {String command, String key, String value}.
					command = hk.getStatus().toString();
					key = hk.getKey();
					value = hk.getValue();
					
					//check validity of request WE MAY HAVE TO REMOVE THIS
					if ( !StatusType.PUT.toString().equalsIgnoreCase(command) && !StatusType.GET.toString().equalsIgnoreCase(command) ) {
						//I didnt receive PUT or GET
						logger.info("message format unknown");
						hk.setStatus(StatusType.PUT_ERROR);
						validity = false;
					}
					/*else if ( !StatusType.GET.toString().equalsIgnoreCase(command) ) {
						//I didnt receive GET
						logger.info("message format unknown");
						hk.setStatus(StatusType.GET_ERROR);
						validity = false;
					}*/
					else if ( (value.length() > 120000 || key.length() > 20) && StatusType.PUT.toString().equalsIgnoreCase(command) ) {	//1 character = 1 byte. Limit is 120Kbyte for value and 20Byte for key
						//check message size
						logger.info("message size exceeded");
						hk.setStatus(StatusType.PUT_ERROR);
						validity = false;
					}
					else if ( (value.length() > 120000 || key.length() > 20) && StatusType.GET.toString().equalsIgnoreCase(command) ) {	//1 character = 1 byte. Limit is 120Kbyte for value and 20Byte for key
						//check message size
						logger.info("message size exceeded");
						hk.setStatus(StatusType.GET_ERROR);
						validity = false;
					}
					/* If we have " put <key> <value> " then we save this combinations in the Array_List or 
					 * update if there is an entry already and then reply to the client 
					 */
					if (validity) {
						if ( command.equalsIgnoreCase("put") ) {
							req_put = db.put(p, key, value);
							
							if ( req_put.equalsIgnoreCase("PUT_SUCCESS") ) {
								//configure "response" in order to notify client that command PUT was executed successfully
								hk.setStatus(StatusType.PUT_SUCCESS);
							}
							else if ( req_put.equalsIgnoreCase("PUT_UPDATE") ) {
								//configure "response" in order to notify client that command PUT was executed successfully
								hk.setStatus(StatusType.PUT_UPDATE);
							}
							else if ( req_put.equalsIgnoreCase("PUT_ERROR") ) {
								//configure "response" in order to notify client that command PUT was executed successfully
								hk.setStatus(StatusType.PUT_ERROR);
							}
							else if ( req_put.equalsIgnoreCase("DELETE_SUCCESS") ) {
								//configure "response" in order to notify client that command PUT was executed successfully
								hk.setStatus(StatusType.DELETE_SUCCESS);
							}
							else if ( req_put.equalsIgnoreCase("DELETE_ERROR") ) {
								//configure "response" in order to notify client that command PUT was executed successfully
								hk.setStatus(StatusType.DELETE_ERROR);
							}
						}
					 
					 /* If we have " get <key> " then we get the value from the Array_List and we 
					 * return the resulting value - message to the client
					 * 
					 * */
						else if ( command.equalsIgnoreCase("get") ) {
							req_get = db.get(p, key);
							
							if ( !req_get.equalsIgnoreCase("ERROR") ) {
								//configure "response" in order to notify client that command GET was executed successfully
								hk.setStatus(StatusType.GET_SUCCESS);
								hk.setValue(req_get);
							}
							else {
								//configure "response" in order to notify client that command GET was NOT executed successfully
								hk.setStatus(StatusType.GET_ERROR);
							}
							
						}
					}
					
					//marshall here
					response = hk.encodeKVMessage();
					sendMessage(new TextMessage(response));
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */

	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	private TextMessage receiveMessage() throws IOException {
		
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
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
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
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
    }
	

}
package app_kvClient;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.MessageProcessing;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;
import client.KVStore;
import client.ClientSocketListener;


public class KVClient implements ClientSocketListener {

        private static Logger logger = Logger.getRootLogger();
        private static final String PROMPT = "kvClient> ";
        private BufferedReader stdin;
        private KVStore client = null;
        private boolean stop = false;
        
        private String serverAddress;
        private int serverPort;
        
        public void run() {
                while(!stop) {
                        stdin = new BufferedReader(new InputStreamReader(System.in));
                        System.out.print(PROMPT);
                        
                        try {
                                String cmdLine = stdin.readLine();
                                this.handleCommand(cmdLine);
                        } catch (IOException e) {
                                stop = true;
                                printError("CLI does not respond - Application terminated ");
                        }
                }
        }
        
        private void handleCommand(String cmdLine) {
                String[] tokens = cmdLine.split("\\s+");

                if(tokens[0].equals("quit")) {  
                        stop = true;
                        disconnect();
                        System.out.println(PROMPT + "Application exit!");
                
                } else if (tokens[0].equals("connect")){
                        if(tokens.length == 3) {
                                try{
                                        serverAddress = tokens[1];
                                        serverPort = Integer.parseInt(tokens[2]);
                                        this.connect(serverAddress, serverPort);
                                } catch(NumberFormatException nfe) {
                                        printError("No valid address. Port must be a number!");
                                        logger.info("Unable to parse argument <port>", nfe);
                                } catch (UnknownHostException e) {
                                        printError("Unknown Host!");
                                        logger.info("Unknown Host!", e);
                                } catch (IOException e) {
                                        printError("Could not establish connection!");
                                        logger.warn("Could not establish connection!", e);
                                } catch (Exception e) {
                                        // THIS WILL BE CHANGED PROBABLY
                                        e.printStackTrace();
                                }
                        } else {
                                printError("Invalid number of parameters!");
                        }       
                } else if (tokens[0].equals("put")) {
                        if(tokens.length == 3) {
                                if(client != null && client.isRunning()){
                                	
                                        this.putKVMessage(tokens[1],tokens[2]);
                                        
                                }
                        }
                        else {
                                printError("Invalid number of parameters!");
                        }
                        /* put <key> <value>
                         * Add tokens[0].equals("put")
                         *This command will split the tokens in key and value and then will call this.putKVMessage
                         *this.putKVMessage will call client.put(key,value) . Check description in KVCommInterface!!!!
                         *
                         *
                         *In general the pattern of the send command must be followed
                         * 
                         * */
                }
                else if (tokens[0].equals("get")) {
                        if(tokens.length == 2) {
                                if(client != null && client.isRunning()){
                                        this.getKVMessage(tokens[1]);
                                }
                        }
                        /* get <key> <value>
                         * Add tokens[0].equals("get <key>")
                         * This command will split get the key and when will call this.getKVMessage
                         * this.getKVMessage will call client.get(key) . Check description in KVCommInterface!!!!
                         * 
                         * In general the pattern of the send command must be followed
                         * 
                         * */
                }
                /*
                else  if (tokens[0].equals("send")) {
                        if(tokens.length >= 2) {
                                if(client != null && client.isRunning()){
                                        StringBuilder msg = new StringBuilder();
                                        for(int i = 1; i < tokens.length; i++) {
                                                msg.append(tokens[i]);
                                                if (i != tokens.length -1 ) {
                                                        msg.append(" ");
                                                }
                                        }       
                                        sendMessage(msg.toString());
                                } else {
                                        printError("Not connected!");
                                }
                        } else {
                                printError("No message passed!");
                        }
                        
                }
                */
                else if(tokens[0].equals("disconnect")) {
                        disconnect();
                        
                } else if(tokens[0].equals("logLevel")) {
                        if(tokens.length == 2) {
                                String level = setLevel(tokens[1]);
                                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                                        printError("No valid log level!");
                                        printPossibleLogLevels();
                                } else {
                                        System.out.println(PROMPT + 
                                                        "Log level changed to level " + level);
                                }
                        } else {
                                printError("Invalid number of parameters!");
                        }
                        
                } else if(tokens[0].equals("help")) {
                        printHelp();
                } else {
                        printError("Unknown command");
                        printHelp();
                }
        }
        
        private void putKVMessage (String key , String value) {
                try {
                	
                        KVMessage responseMessage = client.put(key,value);
                       
                        if (responseMessage.getStatus().equals(StatusType.PUT_SUCCESS)) {
                                System.out.println(PROMPT + "Put was successfull for value " +
                        responseMessage.getValue() + " and key " + responseMessage.getKey());
                        }
                        
                        else if (responseMessage.getStatus().equals(StatusType.PUT_ERROR)) {
                                System.out.println(PROMPT + "Put was not successfull , please try again");
                        }
                        else if (responseMessage.getStatus().equals(StatusType.PUT_UPDATE)) {
                                System.out.println(PROMPT + "The value corresponding to " +
                        responseMessage.getKey() + " was updated successfully with value " + responseMessage.getValue());
                        }
                        else if (responseMessage.getStatus().equals(StatusType.DELETE_SUCCESS)) {
                                System.out.println(PROMPT + "The value corresponding to " +
                        responseMessage.getKey() + " was deleted");
                        }
                        else if (responseMessage.getStatus().equals(StatusType.DELETE_ERROR)) {
                                System.out.println(PROMPT + "The value corresponding to " +
                        responseMessage.getKey() + " was not deleted");
                                
                                
                        }
                } catch (Exception e) {
                        printError("Unable to put KVMessage");
                        disconnect();
                }
                
        }
        
        private void getKVMessage (String key) {
                try {
                        KVMessage responseMessage = client.get(key);
                        ///HandleKVMessage responseMessage = (HandleKVMessage) client.get(key);
                       
                        
                        if (responseMessage.getStatus().equals(StatusType.GET_SUCCESS)) {
                                System.out.println(PROMPT + "Key : " + responseMessage.getKey() + " value is : " + responseMessage.getValue());
                        }
                        
                        else if (responseMessage.getStatus().equals(StatusType.GET_ERROR)) {
                                System.out.println(PROMPT + "There is no value for key : " + responseMessage.getKey());
                        }
                } catch (Exception e) {
                        printError("Unable to get KVMessage");
                        disconnect();
                }
        } 
        /*
        private void sendMessage(String msg){
                try {
                        client.sendMessage(new TextMessage(msg));
                } catch (IOException e) {
                        printError("Unable to send message!");
                        disconnect();
                }
        } */

        private void connect(String address, int port) 
                        throws UnknownHostException, IOException , Exception {
                client = new KVStore(address, port);
                client.addListener(this);
                client.connect();
        }
        
        private void disconnect() {
                if(client != null) {
                        client.disconnect();
                        client = null;
                }
        }
        
        private void printHelp() {
                StringBuilder sb = new StringBuilder();
                sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
                sb.append(PROMPT);
                sb.append("::::::::::::::::::::::::::::::::");
                sb.append("::::::::::::::::::::::::::::::::\n");
                sb.append(PROMPT).append("connect <host> <port>");
                sb.append("\t establishes a connection to a server\n");
                sb.append(PROMPT).append("send <text message>");
                sb.append("\t\t sends a text message to the server \n");
                
                sb.append(PROMPT).append("put <key> <value>");
                sb.append("\t\t Inserts a key-value pair into the storage server data structure.\n \t\t Updates (overwrites) the current value with the given value if the server already contains the specified key.\n \t\tDeletes the entry for the given key if <value> equals null.\n");
                
                sb.append(PROMPT).append("get <key>");
                sb.append("\t\t Retrieves the value for the given key from the storage server. \n");
                
                
                sb.append(PROMPT).append("disconnect");
                sb.append("\t\t\t disconnects from the server \n");
                
                sb.append(PROMPT).append("logLevel");
                sb.append("\t\t\t changes the logLevel \n");
                sb.append(PROMPT).append("\t\t\t\t ");
                sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
                
                sb.append(PROMPT).append("quit ");
                sb.append("\t\t\t exits the program");
                System.out.println(sb.toString());
        }
        
        private void printPossibleLogLevels() {
                System.out.println(PROMPT 
                                + "Possible log levels are:");
                System.out.println(PROMPT 
                                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
        }

        private String setLevel(String levelString) {
                
                if(levelString.equals(Level.ALL.toString())) {
                        logger.setLevel(Level.ALL);
                        return Level.ALL.toString();
                } else if(levelString.equals(Level.DEBUG.toString())) {
                        logger.setLevel(Level.DEBUG);
                        return Level.DEBUG.toString();
                } else if(levelString.equals(Level.INFO.toString())) {
                        logger.setLevel(Level.INFO);
                        return Level.INFO.toString();
                } else if(levelString.equals(Level.WARN.toString())) {
                        logger.setLevel(Level.WARN);
                        return Level.WARN.toString();
                } else if(levelString.equals(Level.ERROR.toString())) {
                        logger.setLevel(Level.ERROR);
                        return Level.ERROR.toString();
                } else if(levelString.equals(Level.FATAL.toString())) {
                        logger.setLevel(Level.FATAL);
                        return Level.FATAL.toString();
                } else if(levelString.equals(Level.OFF.toString())) {
                        logger.setLevel(Level.OFF);
                        return Level.OFF.toString();
                } else {
                        return LogSetup.UNKNOWN_LEVEL;
                }
        }
        
        @Override
        public void handleNewMessage(String msg) {
                if(!stop) {
                        System.out.println(msg);
                }
        }
        
        @Override
        public void handleStatus(SocketStatus status) {
                if(status == SocketStatus.CONNECTED) {

                } else if (status == SocketStatus.DISCONNECTED) {
                        System.out.print(PROMPT);
                        System.out.println("Connection terminated: " 
                                        + serverAddress + " / " + serverPort);
                        
                } else if (status == SocketStatus.CONNECTION_LOST) {
                        System.out.println("Connection lost: " 
                                        + serverAddress + " / " + serverPort);
                        System.out.print(PROMPT);
                }
                
        }

        private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
        }
        
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
                        new LogSetup("logs/client/client.log", Level.OFF);
                        KVClient app = new KVClient();
                        app.run();
                } catch (IOException e) {
                        System.out.println("Error! Unable to initialize logger!");
                        e.printStackTrace();
                        System.exit(1);
                }
    }

}

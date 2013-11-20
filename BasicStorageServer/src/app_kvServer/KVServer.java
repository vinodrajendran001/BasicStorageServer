package app_kvServer;


import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import server.ClientConnection;
import server.Pair;


public class KVServer extends Thread  {
        
private static Logger logger = Logger.getRootLogger();
        
        private int port; 
    private ServerSocket serverSocket; 
    private boolean running;
    List <Pair> p = new ArrayList<Pair>();
        
        /**
         * Start KV Server at given port
         * @param port given port for storage server to operate
         */
        public KVServer(int port) {
                this.port = port;
        }
        
        public void run() {
        
        running = initializeServer();
        
        if(serverSocket != null) {
                while(isRunning()){
                    try {
                        Socket client = serverSocket.accept();                
                        ClientConnection connection = 
                                        new ClientConnection(client,p);
                        new Thread(connection).start();
                        
                        logger.info("Connected to " 
                                        + client.getInetAddress().getHostName() 
                                        +  " on port " + client.getPort());
                    } catch (IOException e) {
                        logger.error("Error! " +
                                        "Unable to establish connection. \n", e);
                    }
                }
        }
        logger.info("Server stopped.");
    }
        
        private boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopServer(){
        running = false;
        try {
                        serverSocket.close();
                } catch (IOException e) {
                        logger.error("Error! " +
                                        "Unable to close socket on port: " + port, e);
                }
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
                        + serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
                logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
    
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
                        new LogSetup("logs/server/server.log", Level.ALL);
                        System.out.println("Testing :"+args.length);
                        if(args.length != 1) {
                                System.out.println("Error! Invalid number of arguments!");
                                System.out.println("Usage: Server <port>!");
                        } else {
                        		
                                int port = Integer.parseInt(args[0]);
                                new KVServer(port).start();
                        }
                } catch (IOException e) {
                        System.out.println("Error! Unable to initialize logger!");
                        e.printStackTrace();
                        System.exit(1);
                } catch (NumberFormatException nfe) {
                        System.out.println("Error! Invalid argument <port>! Not a number!");
                        System.out.println("Usage: Server <port>!");
                        System.exit(1);
                }
    }

}
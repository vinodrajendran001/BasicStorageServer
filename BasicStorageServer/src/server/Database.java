package server;

import java.util.List;

import org.apache.log4j.Logger;

public class Database {

        private static Logger logger = Logger.getRootLogger();
        
        public synchronized String put(List<Data> p, String key, String value) {
                int i = 0;
                Data temp = new Data();
                temp.setKey(key);
                temp.setValue(value);
                
                for ( i = 0; i < p.size(); i++ ) {                                                                                      
                        if ( p.get(i).getKey().equalsIgnoreCase(key) && !temp.getValue().equalsIgnoreCase("null") ) {                                                                                   //update
                                try { p.set(i, temp); logger.info("tuple successfully updated"); return "PUT_UPDATE"; }
                                catch(Exception e) { logger.error("unable to update tuple", e); return "PUT_ERROR"; }
                        }
                }
                
                for ( i = 0; i < p.size(); i++ ) {
                        if (p.get(i).getKey().equalsIgnoreCase(key) && temp.getValue().equalsIgnoreCase("null")) {                                                                                              //delete
                                try { p.remove(i); logger.info("tuple successfully deleted"); return "DELETE_SUCCESS"; }
                                catch(Exception e) { logger.error("unable to delete tuple", e); return "DELETE_ERROR"; }
                        }
                }
                                                                                                                                                                                                                //insertion
                if ( !temp.getValue().equalsIgnoreCase("null") ) {              
                        try { p.add(temp); logger.info("tuple successfully inserted"); return "PUT_SUCCESS"; }
                        catch(Exception e) { logger.error("unable to insert tuple", e); return "PUT_ERROR"; }
                }
                
                return "DELETE_ERROR" ;

        }

        public synchronized String get(List<Data> p, String key) {
                
                for (int i = 0; i < p.size(); i++ ) {
                        if ( p.get(i).getKey().equalsIgnoreCase(key) ) {
                                logger.info("tuple found");
                                return p.get(i).getValue();                                                                                                                             //return indexed value
                        }
                }
                logger.info("tuple not found");
                return "ERROR";                                                                                                                                                                 //given key not present
                
        }

}
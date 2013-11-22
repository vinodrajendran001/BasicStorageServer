package common.messages;

public class MessageProcessing implements KVMessage {
    
    
    StatusType status;
    String key ;
    String value ;

    @Override
    public String getKey() {
            return this.key;
    }
    
    public void setKey(String key) {
            this.key = key ;
    }

    @Override
    public String getValue() {
            return this.value;
    }
    
    public void setValue(String value) {
            this.value = value ;
    }

    @Override
    public StatusType getStatus() {
            return this.status;
    }
    
    public void setStatus (StatusType status) {
            this.status=status;
    }
    
    
    @Override
    public byte[] messageEncoding() {
           
            byte keyString[] = this.key.getBytes();
            
            byte valueString[] = this.value.getBytes();
    
            byte encodedMSG [] = new byte[keyString.length + valueString.length + 3];
           
            encodedMSG[0] = (byte)this.status.ordinal();
        
            encodedMSG[1] = (byte)keyString.length;

            for(int i = 0;i<keyString.length;i++)
                    encodedMSG[i+2] = keyString[i];
            
            encodedMSG[2+keyString.length] = (byte)valueString.length;
   
            for(int i = 0;i<valueString.length;i++)
                    encodedMSG[i + keyString.length + 3] = valueString[i];
           
            return encodedMSG;
    }

    @Override
    public void messageDecoding(byte[] KVMessage) {
           
            this.status = StatusType.values()[KVMessage[0]];
            
            byte keyString[] = new byte[(int)KVMessage[1]];
            
            for(int i = 0;i<keyString.length;i++)
                    keyString[i] = KVMessage[i+2];
       
            this.key = new String(keyString);
            
            byte valueString[] = new byte[(int)KVMessage[2 + keyString.length]];
      
            for(int i = 0;i<valueString.length;i++)
                    valueString[i] = KVMessage[i + keyString.length + 3];
           
            this.value = new String(valueString);
            
    }

}
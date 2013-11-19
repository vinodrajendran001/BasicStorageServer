package common.messages;

public class HandleKVMessage implements KVMessage {
    
    
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
    public byte[] encodeKVMessage() {
            //get the bytes of the key string
            byte keyString[] = this.key.getBytes();
            //get the bytes of the value string
            byte valueString[] = this.value.getBytes();
            //create the byte array to be returned with the total length
            byte encodedMSG [] = new byte[keyString.length + valueString.length + 3];
            //put the status as first byte
            encodedMSG[0] = (byte)this.status.ordinal();
            //length of the first string (key)
            encodedMSG[1] = (byte)keyString.length;
            //copy the bytes of the key string
            for(int i = 0;i<keyString.length;i++)
                    encodedMSG[i+2] = keyString[i];
            //length of the second string (value)
            encodedMSG[2+keyString.length] = (byte)valueString.length;
            //copy the bytes of the value string
            for(int i = 0;i<valueString.length;i++)
                    encodedMSG[i + keyString.length + 3] = valueString[i];
            //put the termination byte
            //encodedMSG[3 + keyString.length + valueString.length] = 0x0D;
            return encodedMSG;
    }

    @Override
    public void decodeKVMessage(byte[] KVMessage) {
            //put the status from the first byte
            this.status = StatusType.values()[KVMessage[0]];
            //the second bytes contains the length of the string of the key
            byte keyString[] = new byte[(int)KVMessage[1]];
            //copy the bytes of the key from the byte array
            for(int i = 0;i<keyString.length;i++)
                    keyString[i] = KVMessage[i+2];
            //get the string from the byte array which was copied before
            this.key = new String(keyString);
            //the next byte contains the length of the value string
            byte valueString[] = new byte[(int)KVMessage[2 + keyString.length]];
            //copy the bytes of the value
            for(int i = 0;i<valueString.length;i++)
                    valueString[i] = KVMessage[i + keyString.length + 3];
            //get the string from the byte array
            this.value = new String(valueString);
    }

}
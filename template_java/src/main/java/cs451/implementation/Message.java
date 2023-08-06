package cs451.implementation;
import java.util.Arrays;
import java.util.Objects;

/** Message class specifies the message format used in the broadcasts, and the corresponding objects are used
 * in the broadcasts/links logics
 * The class specifies following types of messages (message types are encoded as bytes). When messages are encoded and sent
 * to destination, the corresponding packet will contain one of the specified encodings to tell the destination which type
 * it received:
 *<ul>
 *     <li> DATA_MESSAGE=1, denotes actual message which is broadcast delivered
 *     <li> ACK_MESSAGE=2, serves to tell the broadcaster that the receiver acknowledges the reception of the broadcast message
 *     <li> MESSAGE_SIZE=7  (placeholder used to initialize byte array version of message during the Message -> byte[] serialization
 *</ul>
 */

public class Message {
    public static final byte DATA_MESSAGE = 1;
    public static final byte ACK_MESSAGE = 2;
    public static final int MESSAGE_SIZE = 7;
    public static final int NO_SOURCE=0;
    private final int value;
    private  byte dataType;
    private byte sourceSenderId;
    private final byte originalSenderId;

    //TODO: newly added
    private int[] vectorClock;


    /**
     * Constructs message based on the parameters
     * @param value  sequence number of the message
     * @param dataType message type (see the @class description)
     * @param sourceSenderId Id of process relaying the message
     * @param originalSenderId Id of the process which sent the message
     */
    public Message(int value, byte dataType, byte sourceSenderId, byte originalSenderId) {
        this.value = value;
        this.dataType = dataType;
        this.sourceSenderId = sourceSenderId;
        this.originalSenderId=originalSenderId;
    }


    public Message(int value, byte dataType, byte sourceSenderId, byte originalSenderId, int[] vectorClock) {
        this.value = value;
        this.dataType = dataType;
        this.sourceSenderId = sourceSenderId;
        this.originalSenderId=originalSenderId;
        this.vectorClock=vectorClock;
    }
    
    
    public Message(Message otherMessage){
        this.value=otherMessage.getValue();
        this.dataType=otherMessage.dataType;
        this.sourceSenderId=otherMessage.sourceSenderId;
        this.originalSenderId=otherMessage.originalSenderId;
        this.vectorClock=new int[otherMessage.vectorClock.length];
        for (int i=0; i<otherMessage.vectorClock.length; i++) {
            this.vectorClock[i]=otherMessage.vectorClock[i];
        }
    }


    /**
     * Converts the message object into the byte array representation which is used during the UDP sending
     * @param data sequence number of the message
     * @param dataType message type (see the @class description)
     * @param originalSenderId id of the process which originally sent (broadcast) the message
     * @param sourceSenderId id of the process which last sent (relayed) the message
     * @param clock vector clock which needs to be serialized
     * @return byte array corresponding to the byte encoding of the message. Message is encoded in the order of the parameters.
     */
    public static byte[] serializeMessage(int data, byte dataType, byte originalSenderId, byte sourceSenderId, int[] clock) {
        //System.out.printf("Message object to serialize: %d, %d\n",data,dataType);

        byte [] result=new byte[4* clock.length+MESSAGE_SIZE];
        for(int i=0; i< clock.length; i++){
            for (int offset=0; offset<4; offset++){
                int index=4*i+offset;
                result[index]=(byte) ((clock[i] >> (24-offset*8)) & 0xff);
                //System.out.println("Serialized byte "+result[index]);
            }
        }
        result[4* clock.length]=dataType;
        result[4* clock.length+1]=originalSenderId;
        result[4* clock.length+2]=sourceSenderId;
        result[4* clock.length+3]=(byte) ((data >> 24) & 0xff);
        result[4* clock.length+4]=(byte) ((data >> 16) & 0xff);
        result[4* clock.length+5]=(byte) ((data >> 8) & 0xff);
        result[4* clock.length+6]=(byte) (data & 0xff);

        return  result;

        /*return new byte[]{
                dataType,
                originalSenderId, //no original sender in this case
                sourceSenderId,
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) (data & 0xff),
        }; */
        //return ByteBuffer.allocate(MESSAGE_SIZE).put(dataType).putInt(data).fl;
    }


    /*
    public static byte[] serializeMessage(int data, byte dataType, byte originalSenderId, byte sourceSenderId) {
        //System.out.printf("Message object to serialize: %d, %d\n",data,dataType);
        return new byte[]{
                dataType,
                originalSenderId, //no original sender in this case
                sourceSenderId,
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) (data & 0xff),
        };
        //return ByteBuffer.allocate(MESSAGE_SIZE).put(dataType).putInt(data).fl;
    } */

    public static byte[] serializeMessage(Message message){
        //System.out.println("In serialize: clock to serialize: "+Arrays.toString(message.getVectorClock()));
        return serializeMessage(message.getValue(),message.getDataType(),message.getByteOriginalSenderId(),message.getByteSourceSenderId(), message.getVectorClock());
    }

/*    *//**
     * Converts the byte array representation of the Message into the corresponding Message object
     * @param data byte representation of the message
     * @return Message object based on the received data
     *//*
    public static Message deserializeMessage(byte[] data) {
        assert data.length <= MESSAGE_SIZE;
        byte dataType = data[0];
        byte originalSenderId=data[1];
        byte sourceSenderId=data[2];
        int value = ( // NOTE: type cast not necessary for int
                (0xff & data[3] << 24 |
                        (0xff & data[4]) << 16 |
                        (0xff & data[5]) << 8 |
                        (0xff & data[6])
        ));
        return new Message(value, dataType, sourceSenderId, originalSenderId);
    } */

    /**
     * Converts the byte array representation of the Message into the corresponding Message object
     * @param data byte representation of the message
     * @return Message object based on the received data
     */
    public static Message deserializeMessage(byte[] data, int numberOfHosts) {
        assert data.length <= MESSAGE_SIZE+4*numberOfHosts;
        int[] vectorClock=new int[numberOfHosts];
        for(int i=0; i<numberOfHosts; i++){
            int value = ( // NOTE: type cast not necessary for int
                    (0xff & data[4*i]) << 24 |
                            (0xff & data[4*i+1]) << 16 |
                            (0xff & data[4*i+2]) << 8 |
                            (0xff & data[4*i+3])
                    );

            vectorClock[i]=value;
            //System.out.println("Deserialized value: "+vectorClock[i]);
        }

        int offset=4*numberOfHosts;
        byte dataType = data[offset];
        byte originalSenderId=data[offset+1];
        byte sourceSenderId=data[offset+2];
        int value = ( // NOTE: type cast not necessary for int
                (0xff & data[offset+3] << 24 |
                        (0xff & data[offset+4]) << 16 |
                        (0xff & data[offset+5]) << 8 |
                        (0xff & data[offset+6])
                ));
        return new Message(value, dataType, sourceSenderId, originalSenderId, vectorClock);
    }

    /**
     * @return byte valued id of the original sender
     */
    public byte getByteOriginalSenderId(){
        return this.originalSenderId;
    }

    /**
     * @return byte valued id of the source sender (host who is relaying the message)
     */
    public byte getByteSourceSenderId(){return this.sourceSenderId;}


    /**
     * @return int valued id of the original sender
     */
    public int getOriginalSenderId(){
        return this.originalSenderId;
    }


    /**
     * @return int valued id of the source sender
     */
    public int getSourceSenderId() {return this.sourceSenderId;}


    /**
     * @return the sequence number of the message
     */
    public int getValue() {
        return value;
    }


    /**
     * @return data type of the message
     */
    public byte getDataType() {
        //System.out.println("In get data type, value, data type: "+this.value+" "+this.dataType);
        return this.dataType;
    }

    /**
     * Change the sourceSenderId (corresponds to the id of the sender who is relaying the message)
     * @param senderId Id of the sender relaying the message
     */
    public void changeSourceSender(int senderId){
        this.sourceSenderId=(byte)senderId;
    }

    public void changeToDataMessage(){
        if(this.dataType==ACK_MESSAGE){
            this.dataType=DATA_MESSAGE;
        }
        System.out.println("Error, you shouldn't be here");
    }


    public int[] getVectorClock() {
        return vectorClock;
    }

    /**
     * @return Pair\<messageId,originalSenderId\>
     */
    public Pair<Integer,Integer> getMessageInfo(){
        //return Pair.create(this.value,this.getOriginalSenderId());
        return new Pair<>(this.getOriginalSenderId(), this.value);
    }

    /**
     * @return Triple\<messageInfo(),sourceSenderId\, originalSenderId\>
     */
    public Triple<Integer,Integer, Integer> getMessageAndSourceInfo(){
        return Triple.create(this.value, this.getSourceSenderId(), this.getOriginalSenderId());
    }

    public Pair<Integer,Integer> getSourceAndOriginalSender(){
        return new Pair<>(this.getSourceSenderId(), this.getOriginalSenderId());
    }

    //TODO: the following two functions might need to be changed
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return value == message.value
                && dataType == message.dataType
                && originalSenderId == message.originalSenderId
                && sourceSenderId== message.sourceSenderId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType, originalSenderId, sourceSenderId);
    }


    @Override
    public String toString() {
        return "Message{" +
                "value=" + value +
                ", dataType=" + dataType +
                ", sourceSenderId=" + sourceSenderId +
                ", originalSenderId=" + originalSenderId +
                ", vectorClock=" + Arrays.toString(this.vectorClock) +
                '}';
    }

    public String toLogString(){
        return  "d "+this.originalSenderId+" "+this.value+"\n";
    }


    public static int getMessageSize(int hostNumber){
        return 4*hostNumber+MESSAGE_SIZE;
    }
}


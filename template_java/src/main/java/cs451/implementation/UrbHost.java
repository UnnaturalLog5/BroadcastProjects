package cs451.implementation;

import cs451.Host;
import java.util.concurrent.*;

/**
 * Class wrapping each process(host) with the URB semantics
 */
public class UrbHost {
    private static final int TIME_TO_WAIT = 150; //Magic const for timeout between sending
    Host me;

    ConcurrentHashMap<Message, Boolean > urbDeliveredMessages;
    ScheduledExecutorService urbDeliveryService;
    LinkHost linkHost;
    //NOTE: the value of the future will be used to stop broadcasting the message
    ScheduledFuture<?> continueUrbDelivery;


    public Host getMe() {
        return me;
    }

    //Batch prepare the broadcast logging to save space, if something is not delivered, it really doesn't matter,
    /**
     * Constructor for UrbHost
     * @param me Host object denoting the source
     * @param linkHost host doing the PerfectLink delivery semantics
     * @param messageToSendNum number of messages to be Urb Broadcasted
     */
    public UrbHost(Host me, LinkHost linkHost, int messageToSendNum){
        this.me=me;
        this.linkHost=linkHost;
        this.urbDeliveredMessages =new ConcurrentHashMap<>(messageToSendNum);
        this.urbDeliveryService=Executors.newSingleThreadScheduledExecutor();
    }


/*
    */
/**
     * Logs the initial sending (broadcast) of the messages when the protocol begins
     * @param totalMsgToSendNum Number of messages to be broadcast to the network

    private void logBroadcast(int totalMsgToSendNum){
        StringBuilder stringBroadcast=new StringBuilder(3*totalMsgToSendNum);
        for(int i=0; i<totalMsgToSendNum-1; i++){
            stringBroadcast.append("b ").append(i + 1).append("\n");
        }
        stringBroadcast.append("b ").append(totalMsgToSendNum);
        this.me.log(stringBroadcast.toString());
    }


    /** When broadcast call is first issued, we don't send the messages to ourselves over the network, instead we do the
     * ack counting "locally"
     * Logs the initial sending (broadcast) of the messages when the protocol begins
     * @param totalMsgToSendNum Number of messages to be broadcast to the network
     * @param selfSender perfect link for which I am both the sender and the receiver
     *//*

    private void doSelfUrbAck(int totalMsgToSendNum, PerfectLink selfSender){
        //System.out.println("Doing self urb acking");
        byte myId=(byte)this.me.getId();
        for(int i=0; i<totalMsgToSendNum-1; i++){
            //Pair<Integer,Integer> key= new Pair<>(myId, i+1);

            this.linkHost.urbAckCount.put(key, this.linkHost.urbAckCount.getOrDefault(key,0) + 1);
            int [] vc = new int[0];
            new Message(i,Message.DATA_MESSAGE,myId,myId,vc);
            selfSender.deliveredMessages.put(Triple.create(i+1,myId,myId),true);
        }
        Pair<Integer,Integer> key=new Pair<>(myId, totalMsgToSendNum);
        selfSender.deliveredMessages.put(Triple.create(totalMsgToSendNum,myId,myId),true);
        this.linkHost.urbAckCount.put(key, this.linkHost.urbAckCount.getOrDefault(key,0) + 1);
    }

    */
/**
     * A wrapper function called at the beginning (when broadcast is called)
     * @param messagesToSendNum Number of messages to be sent
     *//*

    public void beginBroadcasting(int messagesToSendNum){
        this.logBroadcast(messagesToSendNum);

        this.linkHost.perfectLinks.forEach((destID, link) -> {
            if (destID != this.me.getId()) {
                link.beginSending(messagesToSendNum);
            } else {
                this.doSelfUrbAck(messagesToSendNum, link);
            }
        });

    }
*/

     /**Used for LCB broadcast */

    public void sendMessage(Message message){

        this.linkHost.perfectLinks.forEach((destID, link) -> {
            if (destID != this.me.getId()) {
                //System.out.println("In UrbHost: sending data message: "+message + "to: "+destID);
                link.sendDataMessage(message);
            } else {
                this.doSelfUrbAck(message, link);
            }
        });
    }

    //TODO: deal with this  when fixing delivery
    private void doSelfUrbAck(Message message, PerfectLink selfSender){
        //System.out.println("Doing self urb acking");
        Message keyMessage=new Message(message);
        keyMessage.changeSourceSender(Message.NO_SOURCE);
        this.linkHost.urbAckCount.put(message, this.linkHost.urbAckCount.getOrDefault(keyMessage,0) + 1);
        //new Message(i,Message.DATA_MESSAGE,myId,myId,vc);
        selfSender.deliveredMessages.put(message,true);

    }



    /**
     * Lunches anonymous thread which the ack count maintained be the LinkHost and checks for each message
     * if the Host received the majority of acknowledgments. If this is the case, the message is delivered, and the delivery is logged
     * of the Host me. The incoming messages are handled depending on their type:
     */
    private void beginUrbDelivery(boolean doLogging){
        this.continueUrbDelivery=this.urbDeliveryService.scheduleAtFixedRate(() -> {
            try {
                //NOTE: +1 is needed for both cases (when size is even or size is odd)
                int deliveryThreshold = this.linkHost.perfectLinks.size() / 2 + 1;
                //System.out.println("Trying to urb deliver messages, Delivery threshold is: " +deliveryThreshold);
                //System.out.println("Ack map size is: "+this.linkHost.urbAckCount.size());
                for (var entry : this.linkHost.urbAckCount.entrySet()) {
                    if(this.urbDeliveredMessages.contains(entry.getKey())){
                        //System.out.println("Removing message from ack count because we already delivered it :" + entry.getKey() + ": " + entry.getValue());
                        this.linkHost.urbAckCount.remove(entry.getKey());
                        continue;
                    }

                    //System.out.println("Ack count for :" + entry.getKey() + ": " + entry.getValue());
                    if (entry.getValue() >= deliveryThreshold) {
                        this.urbDeliveredMessages.put(entry.getKey(),Boolean.TRUE);
                        //System.out.println("URB Delivered message" + entry.getKey());
                        this.linkHost.urbAckCount.remove(entry.getKey());
                    }
                }

            } catch ( Exception e ) {
                //System.out.println( "ERROR URB- unexpected exception" );
                e.printStackTrace();
            }

        },TIME_TO_WAIT,TIME_TO_WAIT,TimeUnit.MILLISECONDS); //(TODO: Maybe add more threads to executor, and/or reduce delay)
    }


    public void beginWorking(boolean doLogging){
        this.linkHost.beginReception();
        this.beginUrbDelivery(doLogging);
    }

    public void shutdownHost(){
        this.linkHost.shutdownLinkHost();
        this.urbDeliveryService.shutdownNow();
    }

}

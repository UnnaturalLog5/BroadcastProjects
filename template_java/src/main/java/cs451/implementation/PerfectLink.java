package cs451.implementation;

import cs451.Host;
import java.io.IOException;
import java.net.DatagramPacket;
import java.util.HashMap;
import java.util.concurrent.*;

/**
 * Class which specifies PerfectLink semantics
 */
public class PerfectLink {
    private static final int TIME_TO_WAIT = 150; //Magic const for timeout between sending
    private static final int INITIAL_CAPACITY = 32; //Another magic constant for message buffers (deliveredMessages, unackedMessages)

    Host source;
    Host destination;
    ScheduledExecutorService sendService;
    //int[] sendVectorClock;
    ExecutorService receiveService;
    //We are now putting information of the form: (messageId, sourceSenderId, originalSenderId)
    HashMap<Message, ScheduledFuture<?>> unackedMessages;
    //We are now putting information of the form: (messageId, sourceSenderId, originalSenderId)
    HashMap<Message, Boolean> deliveredMessages;
    //Logger logger;


    /**
     * @param source Sender of messages
     * @param destination Recipient of messages
     */
    public PerfectLink(Host source,Host destination){
        this.source=source;
        this.destination=destination;
        this.sendService=Executors.newSingleThreadScheduledExecutor();
        this.receiveService=Executors.newSingleThreadExecutor();
        this.unackedMessages=new HashMap<>(INITIAL_CAPACITY);
        this.deliveredMessages=new HashMap<>(INITIAL_CAPACITY);




//        this.logger = Logger.getLogger("Logger"+source.getId());
//        //this.logger.setUseParentHandlers(false);
//        //Logger parentLog= logger.getParent();
//        //FileHandler fileHandler;
//        this.logger = Logger.getLogger("Logger"+source.getId());
//        //this.logger.setUseParentHandlers(false);
//        //Logger parentLog= logger.getParent();
//        //FileHandler fileHandler;
//        /*if (parentLog!=null&&parentLog.getHandlers().length>0) {
//            parentLog.removeHandler(parentLog.getHandlers()[0]);
//        } */
//        try {
//            //String filename="process"+source.getId()+".txt";
//            // the following statement is used to log any messages
//            FileHandler handler=new FileHandler(source.getOutputAddr(), NO_LIMIT,1, true);
//            BroadcastFormatter formatter=new BroadcastFormatter();
//            handler.setFormatter(formatter);
//            this.logger.addHandler(handler);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        this.logger.setLevel(Level.OFF);
    }

    /**
     * Sends data message (and keeps resending it) until the acknowledgement is received
     * @param message message to be relayed to the appropriate destination
     */
    public void sendDataMessage(Message message) {
        System.out.println("In sendDataMessage, for message: "+message+" Sent to: "+destination.getId());
        if (!this.unackedMessages.containsKey(message)) {
            //System.out.println("In sendDataMessage (just before scheduling), for message: "+message+" Sent to: "+destination.getId());
            ScheduledFuture<?> isDone = this.sendService.scheduleAtFixedRate(() -> {
                //System.out.println("In sendDataMessage (now within loop), for message: "+message+" Sent to: "+destination.getId());
                //System.out.println("In sendDataMessage: Starting to send packet, serializing: "+message);
                byte[] messageBytes = Message.serializeMessage(message);

                try {
                    //System.out.println("In catch block");
                    DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, destination.getAddress(), destination.getPort());
                    //System.out.println("In sendDataMessage: Sending data message:" + message + " to: " + this.destination.getId());
                    source.getSocket().send(packet);
                } catch (IOException e) {
                    System.err.println("Unable to send a message due to the socket error");
                    e.printStackTrace();
                }
                //return true;
            }, 0, TIME_TO_WAIT, TimeUnit.MILLISECONDS); //(TODO: Maybe add more threads to executor, and/or reduce delay)
            //0Pair<Integer,Integer> msgSenders=message.getSourceAndOriginalSender();
            this.unackedMessages.put(message,isDone);
            //System.out.println("For destination: " +this.destination.getId()+ " After putting new relay message, unackedMessages size: " +this.unackedMessages.size());
        }

    }




    /**
     * Sends ack message (and keeps resending) until the acknowledgement is received
     * @param seqNum sequence number of the ack message which is sent
     * @param originalSenderId who originally sent the message
     */
    public void sendAckMessage(int seqNum, int originalSenderId, int[] vectorClock) {
        //this.sendService.execute();
        this.sendService.execute(() -> {
            //System.out.println("Starting to send packet");
            byte byteOriginalSender=(byte) originalSenderId;
            byte byteSourceSender=(byte) destination.getId();
            byte[] messageBytes = Message.serializeMessage(seqNum, Message.ACK_MESSAGE, byteOriginalSender, byteSourceSender, vectorClock);
            //DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, destination.getPort());
            try {
                //System.out.println("In catch block");
                DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, destination.getAddress(), destination.getPort());
                //Message message = new Message(seqNum, Message.ACK_MESSAGE, byteSourceSender, byteOriginalSender);
                //System.out.println("Sending ack packet:" + message+" to: "+this.destination.getId());
                source.getSocket().send(packet);
            } catch (IOException e) {
                System.err.println("Unable to send a message due to the socket error");
                e.printStackTrace();
            }

        });//(TODO: Maybe add more threads to executor, and/or reduce delay)
    }


    /**
     * Delivers message received by the source and logs the delivery in the log file
     * @param message Message to deliver and log it's delivery.
     */
    public void deliverMessage(Message message){
        //this.source.log("d "+message.getByteSenderId()+" "+message.getValue());
        this.deliveredMessages.put(message,true);
        //System.out.println("Via PerfectLink: Delivered: "+message);
    }

    public void shutdownLink(){
        this.receiveService.shutdownNow();
        this.sendService.shutdownNow();

    }



}

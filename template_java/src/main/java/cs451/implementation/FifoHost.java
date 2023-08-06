package cs451.implementation;

import cs451.Host;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;



/**
 * Class wrapping each process(host) with the URB semantics
 */
public class FifoHost {
    private static final int INITIAL_CAPACITY=32;
    private static final int TIME_TO_WAIT=150;
    public Host me;
    public UrbHost urbHost;
    public HashSet<Pair<Integer,Integer> > fifoDeliveredMessages;
    public HashMap<Integer,Integer> lastDeliveredMessageId;
    ScheduledExecutorService fifoDeliveryService;
    ScheduledFuture<?> continueFifoDelivery;


    /**
     * Constructor for UrbHost
     * @param urbHost urbHost which is used as the basis for FIFO delivery
     */
    public FifoHost(UrbHost urbHost){
        this.me=urbHost.me;
        this.urbHost=urbHost;
        this.fifoDeliveryService= Executors.newSingleThreadScheduledExecutor();
        this.fifoDeliveredMessages=new HashSet<>(INITIAL_CAPACITY);
        this.setupLastDeliveredMessagesMap();
    }

    /**
     * Private function used to setupTheCounters (IDs) of the last delivered message ID for each of the participants
     */
    private void setupLastDeliveredMessagesMap(){
        this.lastDeliveredMessageId=new HashMap<>(this.urbHost.linkHost.perfectLinks.size());
        for(Integer hostID: this.urbHost.linkHost.perfectLinks.keySet()){
            this.lastDeliveredMessageId.put(hostID,0);
        }
        this.lastDeliveredMessageId.putIfAbsent(me.getId(),0);
    }

    /**
     * Lunches anonymous thread which looks into the urbDelivered messages, and tries to deliver messages according to FIFO
     * semantics (i.e. we check if each subsequent messageID satisfies lastSequenceNumber + 1 == messageSeqNum
     */
     private void beginFifoDelivery(boolean doLogging){
        this.continueFifoDelivery=this.fifoDeliveryService.scheduleAtFixedRate(() -> {
            //tryDelivery(true);
            //System.out.println("Trying to FIFO deliver messages");
            StringBuilder deliveryLogger = new StringBuilder();

            try {  //Iterate over all urb delivered messages
                if(this.urbHost.urbDeliveredMessages.size()>0) {

                    for(Integer originalSenderId:this.lastDeliveredMessageId.keySet()) {//Iterate over all other participants in the network
                        int lastSequenceNumber = this.lastDeliveredMessageId.get(originalSenderId);
                        while(true){
                            Pair<Integer,Integer> candidateMessageID=new Pair<>(originalSenderId,lastSequenceNumber+1);
                            if(this.urbHost.urbDeliveredMessages.containsKey(candidateMessageID)){
                                lastSequenceNumber++;
                                //System.out.println("FIFO Delivered message: " + candidateMessageID);
                                if (doLogging) {
                                    deliveryLogger.append("d ").append(candidateMessageID.toLogString());
                                }
                            }
                            else{
                                this.lastDeliveredMessageId.put(originalSenderId,lastSequenceNumber);
                                break;
                            }
                        }
                    }
                    /*for (Pair<Integer, Integer> deliveredMessageId : this.urbHost.urbDeliveredMessages.keySet().stream().sorted().collect(Collectors.toList())) {

                        int originalSenderId = deliveredMessageId.first;
                        int messageSeqNum = deliveredMessageId.second;

                        int lastSequenceNumber = this.lastDeliveredMessageId.get(originalSenderId);
                        if (lastSequenceNumber + 1 == messageSeqNum) {
                            this.lastDeliveredMessageId.put(originalSenderId, messageSeqNum);
                            System.out.println("FIFO Delivered message" + deliveredMessageId);
                            if (doLogging) {
                                deliveryLogger.append("d ").append(deliveredMessageId.toLogString());
                            }
                        }

                    }*/
                }
                /*
                //TODO: JUST FOR TESTING, CLEAN THIS UP
                if(this.fifoDeliveredMessages.size()==7*1000){
                    System.out.println("EVERYTHING IS DELIVERED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                } */

                if (doLogging && deliveryLogger.length()>0) {
                    deliveryLogger.setLength(deliveryLogger.length() - 1);
                    this.me.log(deliveryLogger.toString());
                }

            } catch ( Exception e ) {
                //System.out.println( "ERROR in FIFO- unexpected exception" );
                if (doLogging && deliveryLogger.length()>0) {
                    deliveryLogger.setLength(deliveryLogger.length() - 1);
                    this.me.log(deliveryLogger.toString());
                }
                e.printStackTrace();
            }

        },TIME_TO_WAIT,TIME_TO_WAIT, TimeUnit.MILLISECONDS); //(TODO: Maybe add more threads to executor, and/or reduce delay)
    }

    public void beginWorking(boolean doLogging){
        //false-> don't do urb logging
        this.urbHost.beginWorking(false);
        this.beginFifoDelivery(doLogging);
    }


    public void shutdownHost(){
        this.urbHost.shutdownHost();
    }

}

package cs451.implementation;

import cs451.Host;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LcbHost {
    private static final int INITIAL_CAPACITY=32;
    private static final int TIME_TO_WAIT=150;
    private static final int WINDOW_SIZE=100;
    public Host me;
    public UrbHost urbHost;
    public HashSet<Pair<Integer,Integer> > lcbDeliveredMessages;

    public int[] receiveVectorClock;
    public int[] sendVectorClock;
    ScheduledExecutorService lcbDeliveryService;
    HashSet<Integer> dependencies;
    ScheduledFuture<?> continueLcbDeliveryService;
    AtomicInteger lastSentId;
    AtomicInteger lastDeliveredId;
    StringBuilder deliveryLogger;


    /**
     * Constructor for UrbHost
     * @param urbHost urbHost which is used as the basis for FIFO delivery
     */
    public LcbHost(UrbHost urbHost, HashSet<Integer> dependencies){
        this.me=urbHost.me;
        this.urbHost=urbHost;
        this.lcbDeliveryService= Executors.newSingleThreadScheduledExecutor();
        this.lcbDeliveredMessages=new HashSet<>(INITIAL_CAPACITY);
        this.dependencies=dependencies;
        this.setupAuxiliaryDataStructure();
        this.lastSentId=new AtomicInteger(0);
        this.lastDeliveredId=new AtomicInteger(0);
        this.deliveryLogger = new StringBuilder();
    }


    private void setupAuxiliaryDataStructure(){
        int size=this.urbHost.linkHost.perfectLinks.size();
        this.receiveVectorClock=new int[size];
       this.sendVectorClock=new int[size];
    }


    /**
     * Lunches anonymous thread which looks into the urbDelivered messages, and tries to deliver messages according to FIFO
     * semantics (i.e. we check if each subsequent messageID satisfies lastSequenceNumber + 1 == messageSeqNum
     */
    private void beginLcbDelivery(boolean doLogging){
        this.continueLcbDeliveryService=this.lcbDeliveryService.scheduleAtFixedRate(() -> {
            //tryDelivery(true);
            //System.out.println("Trying to LCB deliver messages");


            try {  //Iterate over all urb delivered messages
                if(this.urbHost.urbDeliveredMessages.size()>0) {

                    for(Message message: this.urbHost.urbDeliveredMessages.keySet()) {//Iterate over all other participants in the network
                        if (!this.lcbDeliveredMessages.contains(message.getMessageInfo())&&this.compareVectorClocks(message)) {
                            this.lcbDeliveredMessages.add(message.getMessageInfo());
                            synchronized (deliveryLogger) {
                                deliveryLogger.append(message.toLogString());
                            }
                            //System.out.println("LCB Delivered: "+message.getMessageInfo()+" "+Arrays.toString(message.getVectorClock()));
                            this.receiveVectorClock[message.getOriginalSenderId()-1]++;
                            if(this.dependencies.contains(message.getOriginalSenderId())){
                                synchronized (this.sendVectorClock) {
                                    this.sendVectorClock[message.getOriginalSenderId() - 1]++;
                                }
                            }
                            if(message.getOriginalSenderId()==this.me.getId()){
                                this.lastDeliveredId.incrementAndGet();
                            }
                        }
                    }
                    if (doLogging && deliveryLogger.length()>0) {
                        synchronized (deliveryLogger) {
                            deliveryLogger.setLength(deliveryLogger.length() - 1);
                            this.me.log(deliveryLogger.toString());
                            deliveryLogger.setLength(0);
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



            } catch ( Exception e ) {
                //System.out.println( "ERROR in FIFO- unexpected exception" );
                if (doLogging && deliveryLogger.length()>0) {
                    synchronized (this.deliveryLogger) {
                        deliveryLogger.setLength(deliveryLogger.length() - 1);
                        this.me.log(deliveryLogger.toString());

                        //Reset string builder
                        deliveryLogger.setLength(0);
                    }
                }
                e.printStackTrace();
            }

        },TIME_TO_WAIT,TIME_TO_WAIT, TimeUnit.MILLISECONDS); //(TODO: Maybe add more threads to executor, and/or reduce delay)
    }

    public void beginWorking(boolean doLogging){
        //false-> don't do urb logging
        this.urbHost.beginWorking(false);
        this.beginLcbDelivery(doLogging);
    }

    public void beginBroadcasting(int messagesToSendNum) {
        int i = 1;
        int init_send = Math.min(messagesToSendNum, WINDOW_SIZE);
        for (; i <= init_send; i++) {
            synchronized (this.sendVectorClock) {
                int[] vcToSend = new int[this.sendVectorClock.length];
                System.arraycopy(this.sendVectorClock, 0, vcToSend, 0, this.sendVectorClock.length);
                Message messageToSend = new Message(i, Message.DATA_MESSAGE, (byte) this.me.getId(), (byte) this.me.getId(), vcToSend);
                //System.out.println("Doing LCB send: " + messageToSend);
                synchronized (this.deliveryLogger){
                    this.deliveryLogger.append("b "+i+"\n");
                }
                this.urbHost.sendMessage(messageToSend);
                this.sendVectorClock[this.me.getId() - 1]++;
            }
        }
        this.lastSentId.set(init_send);
        while (i <= messagesToSendNum) {
            int ub = this.lastSentId.get();
            int lb = this.lastDeliveredId.get();
            int currentWindow = ub - lb;
            if (currentWindow < WINDOW_SIZE) {
                //System.out.println("Have room to send, sending message");
                synchronized (this.sendVectorClock) {
                    int[] vcToSend = new int[this.sendVectorClock.length];
                    System.arraycopy(this.sendVectorClock, 0, vcToSend, 0, this.sendVectorClock.length);
                    Message messageToSend = new Message(i, Message.DATA_MESSAGE, (byte) this.me.getId(), (byte) this.me.getId(), vcToSend);
                    //System.out.println("Doing LCB send: " + messageToSend);
                    synchronized (this.deliveryLogger){
                        this.deliveryLogger.append("b "+i+"\n");
                    }
                    this.urbHost.sendMessage(messageToSend);
                    this.sendVectorClock[this.me.getId() - 1]++;
                }
                this.lastSentId.incrementAndGet();
                i++;
            }
        }
    }


    public boolean compareVectorClocks(Message message){
        int [] receivedClock=message.getVectorClock();
        for(int i=0; i<message.getVectorClock().length; i++){
            if(receivedClock[i]>this.receiveVectorClock[i]){
                return false;
            }
        }
        return true;
    }


    public void shutdownHost(){
        this.urbHost.shutdownHost();
    }

}


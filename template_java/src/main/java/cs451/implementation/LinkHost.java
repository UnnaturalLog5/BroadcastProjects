package cs451.implementation;

import cs451.Host;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * Class wrapping each process(host) with the PerfectLink semantics
 */
public class LinkHost {
    Host me;
    Host destination;

    //Maps hostID to corresponding perfect link whose destination is hostID
    HashMap<Integer, PerfectLink>  perfectLinks;
    ConcurrentHashMap<Message,Integer> urbAckCount;


    /**
     * Constructor for LinkHost.
     *
     * @param me Host object denoting the source
     * @param perfectLinks Map of the PerfectLinks of the form destinationID->PerfectLink(me, destination)
     * @param messagesToSendNum Number of messages to be sent (used when initializing map containing ack count, which is used for URB delivery)
     */

    public LinkHost(Host me, HashMap<Integer, PerfectLink> perfectLinks, int messagesToSendNum){
        this.me=me;
        this.perfectLinks=perfectLinks;
        this.urbAckCount=new ConcurrentHashMap<>(this.perfectLinks.size()*messagesToSendNum);
    }

    /**
     * Constructor for LinkHost.
     *
     * @param me Host object denoting the source
     * @param destination Host object denoting the destination (i.e. other part of the PerfectLink)
     * @param perfectLinks Map of the PerfectLinks of the form destinationID->PerfectLink(me, destination)
     */
    public LinkHost(Host me,Host destination, HashMap<Integer, PerfectLink> perfectLinks){
        this.me=me;
        this.destination=destination;
        this.perfectLinks=perfectLinks;
    }

    /**
     * Lunches anonymous thread which listens on the  designated port number and accepts incoming messages in the name
     * of the Host me. The incoming messages are handled depending on their type:
     * <ul>
     *     <li> Data messages are acknowledged via the matching ack message.
     *     <li> Ack messages signifies that the corresponding data message should not be sent anymore.
     * </ul>
     *
     * Additionally, this part also counts the number of acknowledgments by other host for each message issues in network.
     * This is later on used in the URB portion of the code
     */

    //BIG TODO: There is issue here, for both messages?, we need to a different way of getting the perfect link (if a source is 1, and I got ack from 2, here it still pulls out the perfect link with destination 1)
    public void beginReception(){
        new Thread(() -> {
            //System.out.println("Beginning reception: ");
            while(true){
                try {
                    int messageSize=Message.getMessageSize(this.perfectLinks.size());
                    byte[] data=new byte[messageSize];
                    DatagramPacket receivedPacket=new DatagramPacket(data,Message.getMessageSize(this.perfectLinks.size()));
                    me.getSocket().receive(receivedPacket);
                    Message receivedMessage=Message.deserializeMessage(receivedPacket.getData(),this.perfectLinks.size());
                    //System.out.println("Received something new, message : "+receivedMessage);

                    //Get who is the last sender of the message

                    switch (receivedMessage.getDataType()){
                        case Message.DATA_MESSAGE:
                            //System.out.println("Receiving data message:" + receivedMessage+"from "+receivedMessage.getSourceSenderId());
                            PerfectLink perfectLink=this.getPerfectLinkFromID(receivedMessage.getSourceSenderId());
                            var value =perfectLink.deliveredMessages.putIfAbsent(receivedMessage,true);
                            if(value==null) {//if this is null, this means that we have indeed added something new, so we deliver new message
                                //System.out.println("About to PerfectLink deliver a new message: "+receivedMessage);
                                Message keyMessage=new Message(receivedMessage);
                                keyMessage.changeSourceSender(Message.NO_SOURCE);
                                //receivedMessage.changeSourceSender(Message.NO_SOURCE);
                                //Pair<Integer,Integer> receivedMessageInfo=receivedMessage.getMessageInfo();
                                Integer oldValue=this.urbAckCount.getOrDefault(keyMessage,0); //There shouldn't be concurrency issues, because only one thread operates
                                //System.out.println("About to deliver a new message: "+receivedMessage);
                                //System.out.println("for message: "+receivedMessage+" Ack count for this message was: "+oldValue);

                                //Update ackCount for this message
                                this.urbAckCount.put(keyMessage,oldValue+1);
                                perfectLink.deliverMessage(receivedMessage);
                                this.broadcastRelayMessage(receivedMessage);
                            }
                            perfectLink.sendAckMessage(receivedMessage.getValue(),receivedMessage.getOriginalSenderId(),receivedMessage.getVectorClock());
                            break;
                        case Message.ACK_MESSAGE: //Note: this is acknowledgment for perfect links, which tells me to stop sending the given message

                            //TODO: This is a new perfect link, which we find based on from whom did we receive the message!!!
                            PerfectLink perfectLink1 = getPerfectLinkFromPort(receivedPacket.getPort());
                            //System.out.println("Receiving ack message: "+receivedMessage+" And considering perfect link: "+perfectLink1.destination.getId());
                            //System.out.println("Printing out the unacked messages keys, set size is: "+perfectLink1.unackedMessages.keySet().size());
                            //perfectLink1.unackedMessages.keySet().forEach(key->System.out.println(key));
                            receivedMessage.changeToDataMessage();
                            ScheduledFuture<?> isDone=perfectLink1.unackedMessages.get(receivedMessage);

                            //TODO: Maybe I don't even need this, an ack is sent only if I am already sending, if it is not there, I should just ignore it because I already removed it
                            if(isDone!=null){
                                //System.out.println("Going to cancel the corresponding data message, where the ack was: "+receivedMessage);
                                isDone.cancel(true);
                                perfectLink1.unackedMessages.remove(receivedMessage);
                                //System.err.println("Potential error in stopping the sending for message: "+queryMessageInfo);
                            }
                            break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    /**
     * Sends the received message to other participants using perfect link semantics
     * @param oldRelayMessage Message which is to be relayed to other participants
     */
    public void broadcastRelayMessage(Message oldRelayMessage){
        //Note: Extract the useful information about the message we are relaying
        //System.out.println("Old (relay) message was: "+oldRelayMessage);

        //Note: Here  message data is changed so that I am the one who is sending the relay message
        Message newRelayMessage=new Message(oldRelayMessage);
        newRelayMessage.changeSourceSender(this.me.getId());

        //relayMessage.changeSourceSender(this.me.getId());
        //System.out.println("New (relay) message is: "+newRelayMessage);

        for(PerfectLink link:this.perfectLinks.values()){

            //Note: If the corresponding old message was delivered, or if I have already sent the relay, I don't do another sending of the same message
            if(!link.deliveredMessages.containsKey(oldRelayMessage) && !link.unackedMessages.containsKey(newRelayMessage) ) {

                //Note: I am not sending the message to myself, and also I am not sending the message to the original issuer of the message
                if (link.destination.getId() != newRelayMessage.getSourceSenderId()){
                    System.out.println("Relaying message: " + newRelayMessage + "to " + link.destination.getId());
                    link.sendDataMessage(newRelayMessage);
                }
                else{
                    //Note: This is self-delivery, I immediately deliver the relayed message to myself (if necessary)
                    //TODO:(refactor as a separate function to avoid confusion)
                    Boolean value =link.deliveredMessages.putIfAbsent(newRelayMessage,true);
                    if(value==null) {//if this is null, this means that we have indeed added something new, so we deliver new message
                        //Pair<Integer,Integer> receivedMessageInfo=newRelayMessage.getMessageInfo();
                        Message keyMessage=new Message(newRelayMessage);
                        keyMessage.changeSourceSender(Message.NO_SOURCE);
                        Integer oldValue=this.urbAckCount.getOrDefault(keyMessage,0); //There shouldn't be concurrency issues, because only one thread operates
                        //System.out.println("About to PerfectLink deliver a new message: "+receivedMessage);
                        //System.out.println("for message: "+relayMessage+" Ack count for this message was: "+oldValue);
                        this.urbAckCount.put(keyMessage,oldValue+1);
                        link.deliverMessage(newRelayMessage);
                    }
                }
            }
        }
    }

    /*private void printDeliveredMessages(PerfectLink link){
        System.out.println("Already acked messages with destination being: "+link.destination.getId());
        link.deliveredMessages.keySet().forEach(key->System.out.println(key));
        System.out.println("End");
    } */


    /**
     * Obtains PerfectLink object such that the destination Host of the PerfectLink has destinationID
     * @param destinationID ID of the destinationHost to obtain
     * @return corresponding PerfectLink object
     */
    public PerfectLink getPerfectLinkFromID(int destinationID) {
        return perfectLinks.get(destinationID);
    }

    /**
     * Obtains PerfectLink object such that the destination Host of the PerfectLink has given poer
     * @param port port number of the destinationHost to obtain
     * @return corresponding PerfectLink object
     */
    public PerfectLink getPerfectLinkFromPort(int port) throws UnknownHostException {
        for (PerfectLink link : perfectLinks.values()) {
            if (link.destination.getPort() == port) {
                return link;
            }
        }
        //System.err.println("Warning, if you reached here, you queried a non existing address, likely a sign of error in this function implementation");
        //System.err.println("Address was: "+port);
        return Optional.<PerfectLink>empty().get();
    }

    public void shutdownLinkHost(){
        for(PerfectLink perfectLink: perfectLinks.values()){
            perfectLink.shutdownLink();
        }
    }

}

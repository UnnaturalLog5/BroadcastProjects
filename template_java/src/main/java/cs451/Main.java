package cs451;

import cs451.implementation.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;


public class Main {

    private static void handleSignal(LcbHost source) {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        source.shutdownHost();

        //write/flush output file if necessary
        System.out.println("Writing output.");
        source.me.closeBroadcastLogger();
        //source.closeSocket();
    }

    private static void initSignalHandlers(LcbHost source) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> handleSignal(source)));
    }



    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();



        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");
        Optional<Host> sourceOptional=parser.hosts().stream()
                .filter(host -> host.getId()== parser.myId())
                .findFirst();

        //Configure details of the source

        if(sourceOptional.isEmpty()){
            System.exit(0);
        }
        Host source=sourceOptional.get();
        source.setOutputAddr(parser.output());
        source.setBroadcastLogger();

        try {
            source.setSocket();

        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }



        //Read configuration information for properly setting perfect links (destination and number of messages to send)
        Scanner scanner=null;
        try {
            System.out.println(parser.config());
            scanner=new Scanner(new File(parser.config()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        assert scanner != null;
        String configLine;
        int MESSAGES_TO_SEND=scanner.nextInt();
        int i=0;
        while(i<source.getId()){
            scanner.nextLine();
            i++;
        }
        configLine=scanner.nextLine();
        Set<Integer> dependencies = Arrays.stream(configLine.split(" "))
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
        scanner.close();

        //Remove myself from consideration
        dependencies.remove(source.getId());
        HashSet<Integer> dependSet=new HashSet(dependencies);

        System.out.println("Config line was: "+configLine);
        System.out.println("Config set is: "+dependencies);


        //Create perfect links to other hosts
        //Maps destinationID -> PerfectLink used to send to that destination
        HashMap <Integer, PerfectLink> perfectLinks=new HashMap<>(parser.hosts().size());

        //TODO: We don't filter out ourselves anymore, instead we use our perfect link to deliver
        //.filter(host -> host.getId() != parser.myId())
        for (Host host : parser.hosts()) {
            perfectLinks.put(host.getId(), new PerfectLink(source, host));
        }
        LinkHost linkHost=new LinkHost(source,perfectLinks,MESSAGES_TO_SEND);
        UrbHost urbHost=new UrbHost(source,linkHost,MESSAGES_TO_SEND);
        LcbHost lcbHost=new LcbHost(urbHost,dependSet);
        initSignalHandlers(lcbHost);
        //boolean  doLogging=true;
        //System.out.println("TEST TEST");
        //int [] vc={1025,10000,30000};
        //Message messageToSend=new Message(i,Message.DATA_MESSAGE,(byte)1,(byte)1,vc);
        //System.out.println(messageToSend);
        //byte[] data=Message.serializeMessage(messageToSend);
        //Message deserializedMessage=Message.deserializeMessage(data,3);
        //System.out.println("Deserialized message"+deserializedMessage);


        lcbHost.beginWorking(true);
        System.out.println("Broadcasting and delivering messages...\n");
        lcbHost.beginBroadcasting(MESSAGES_TO_SEND);

        /*This part was used for PerfectLink implementation */

        /*
        Optional<Host> destinationOptional=parser.hosts().stream()
                .filter(host -> host.getId()== destinationID)
                .findFirst();
        if(destinationOptional.isEmpty()){
            System.exit(0);
        }

        Host destination=destinationOptional.get();

        LinkHost sourceLinkHost=new LinkHost(source,destination,perfectLinks);

        //Finally, set handler (i.e. force PrintWriter to flush the output and close resource in the case of a stoppage)
        initSignalHandlers(sourceLinkHost);

        sourceLinkHost.beginReception();



        System.out.println("Broadcasting and delivering messages...\n");
        if(destination.getId()!=sourceLinkHost.getMe().getId()) //Added so that receiver does not send messages to itself
            sourceLinkHost.getPerfectLinkFromID(destinationID).beginSending(MESSAGES_TO_SEND);

         */

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}

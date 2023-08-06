package cs451;

import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;


public class Host {

    private static final String IP_START_REGEX = "/";
    private int id;
    private String ip;
    private int port = -1;
    private DatagramSocket socket;
    private String outputAddr;
    private PrintWriter writer=null;

    public boolean populate(String idString, String ipString, String portString) {
        try {
            id = Integer.parseInt(idString);

            String ipTest = InetAddress.getByName(ipString).toString();
            if (ipTest.startsWith(IP_START_REGEX)) {
                ip = ipTest.substring(1);
            } else {
                ip = InetAddress.getByName(ipTest.split(IP_START_REGEX)[0]).getHostAddress();
            }

            port = Integer.parseInt(portString);
            if (port <= 0) {
                System.err.println("Port in the hosts file must be a positive number!");
                return false;
            }


        } catch (NumberFormatException e) {
            if (port == -1) {
                System.err.println("Id in the hosts file must be a number!");
            } else {
                System.err.println("Port in the hosts file must be a number!");
            }
            return false;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return true;
    }

    public InetAddress getAddress() throws UnknownHostException {
        return InetAddress.getByName(this.ip);
    }

    public void setSocket() throws UnknownHostException, SocketException {
        this.socket=new DatagramSocket(port,this.getAddress());
        System.out.println("Created socket ");
    }

    public String getOutputAddr() {
        return outputAddr;
    }

    public void setOutputAddr(String outputAddr) {
        this.outputAddr = outputAddr;
    }

    public void setBroadcastLogger() {
        try {
            this.writer= new PrintWriter(new BufferedOutputStream(new FileOutputStream(this.outputAddr)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void log(String logString){
        this.writer.println(logString);
    }

    public void closeBroadcastLogger(){
        this.writer.flush();
        this.writer.close();
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    public int getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public void closeSocket(){
        if(!this.socket.isClosed())
            this.socket.close();
    }

}

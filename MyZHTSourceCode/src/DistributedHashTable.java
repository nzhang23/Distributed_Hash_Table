/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author zn
 */
public class DistributedHashTable {

    /**
     * @param args the command line arguments
     */
    private PeertoPeer PeerServer;
    private int port;
    public int ServerNum;//the number of servers
    private SimpleMD5 convert;
    public ArrayList<String> ServerList;//storing the server ip and port being deploied
    public Map<String, Socket> SocketList;//storing the socket connection
    public Map<String, ObjectOutputStream> outList;//storing the outputstream of the socket connection
    public Map<String, ObjectInputStream> inList;//storing the inputstream of the socket connection
    public Parser IDextracter;

    /**
     *
     * @param port
     */
    public DistributedHashTable(int port) {
        this.port = port;
        this.IDextracter = new Parser();
        this.ServerList = this.IDextracter.configure();
        this.ServerNum = this.IDextracter.getNumItems(this.ServerList);
        this.convert = new SimpleMD5();
        this.PeerServer = new PeertoPeer(port);
        this.PeerServer.start();
        this.SocketList = new HashMap<String, Socket>();
        this.outList = new HashMap<String, ObjectOutputStream>();
        this.inList = new HashMap<String, ObjectInputStream>();
        if (this.PeerServer.isAlive()) {
            System.out.println("Server is established!");
        }

    }

    /**
     * implement the put function of this distributed hash table
     *
     * @param key
     * @param value
     * @return
     */
    public boolean put(String key, String value) {
        try {
            String key1 = new String();
            try {
                key1 = convert.MD5(key);//using MD5 algorithm to transfer key into 16 bytes string
            } catch (NoSuchAlgorithmException ex) {
                Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
            }
            BigInteger keyInteger = new BigInteger(key1, 16);//transfer key1 into biginteger
            String num = Integer.toString(this.ServerNum);
            BigInteger Bignum = new BigInteger(num);
            BigInteger id;
            // System.out.println(keyInteger);
            id = keyInteger.mod(Bignum);
            int SelectedId = id.intValue();
            int SelectedPort = Integer.parseInt(this.IDextracter.get_port_number(this.ServerList.get(SelectedId)));//getting the port of server into which you put(key,value)
            String SelectedIP = this.IDextracter.get_ip_string(this.ServerList.get(SelectedId));//getting the ip of server into which you put(key,value)

            String serverID = this.ServerList.get(SelectedId);
            if (SocketList.get(serverID) == null) {//if not making the connection to this server, make a new connection to this server
                Socket PeertoServerSocket = new Socket(SelectedIP, SelectedPort);//making a new connection to this server
                SocketList.put(serverID, PeertoServerSocket);//putting the socket connection into SocketList
                ObjectOutputStream out = new ObjectOutputStream(PeertoServerSocket.getOutputStream());
                outList.put(serverID, out);
                ObjectInputStream in = new ObjectInputStream(PeertoServerSocket.getInputStream());
                inList.put(serverID, in);
                MessageCommand msg = new MessageCommand(Command.PUT, key, value);
                out.writeObject(msg);//sending the put message to the server
                out.flush();
                msg = (MessageCommand) in.readObject();//receiving the message from server

            } else {

                MessageCommand msg = new MessageCommand(Command.PUT, key, value);
                outList.get(serverID).writeObject(msg);//the connection to this server exists in the SocketList, just use this connection
                outList.get(serverID).flush();
                msg = (MessageCommand) inList.get(serverID).readObject();

            }
            return true;
        } catch (IOException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * implement the get function of this distributed hash table
     *
     * @param key
     * @return
     */
    public String get(String key) {
        try {

            String key1 = new String();
            key1 = convert.MD5(key);
            String value = new String();
            BigInteger keyInteger = new BigInteger(key1, 16);
            String num = Integer.toString(this.ServerNum);
            BigInteger Bignum = new BigInteger(num);
            BigInteger id;

            id = keyInteger.mod(Bignum);
            int SelectedId = id.intValue();
            int SelectedPort = Integer.parseInt(this.IDextracter.get_port_number(this.ServerList.get(SelectedId)));
            String SelectedIP = this.IDextracter.get_ip_string(this.ServerList.get(SelectedId));
            String serverID = this.ServerList.get(SelectedId);

            if (SocketList.get(serverID) == null) {
                Socket PeertoServerSocket = new Socket(SelectedIP, SelectedPort);
                SocketList.put(serverID, PeertoServerSocket);

                ObjectOutputStream out = new ObjectOutputStream(PeertoServerSocket.getOutputStream());
                outList.put(serverID, out);
                ObjectInputStream in = new ObjectInputStream(PeertoServerSocket.getInputStream());
                inList.put(serverID, in);
                MessageCommand msg = new MessageCommand(Command.GET, key, value);
                out.writeObject(msg);//sending the get message to the server
                out.flush();
                msg = (MessageCommand) in.readObject();//receiving the  message from the server
                if (msg.getCmd() != Command.EMPTY) { // if the command is EMPTY, the key does not exist in this hash table
                    value = (String) msg.getValueBody();
                    return value;
                } else {
                    return null;
                }
            } else {
                MessageCommand msg = new MessageCommand(Command.GET, key, value);
                outList.get(serverID).writeObject(msg);//sending the get message to the server
                outList.get(serverID).flush();
                msg = (MessageCommand) inList.get(serverID).readObject();//receiving the  message from the server

                if (msg.getCmd() != Command.EMPTY) {
                    value = (String) msg.getValueBody();
                    return value;
                } else {
                    return null;
                }

            }

        } catch (IOException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * implement the delete function of this distributed hash table
     *
     * @param key
     * @return
     */
    public boolean delete(String key) {
        try {
            String key1 = new String();
            key1 = convert.MD5(key);
            // System.out.println(key1);
            BigInteger keyInteger = new BigInteger(key1, 16);
            String num = Integer.toString(this.ServerNum);
            BigInteger Bignum = new BigInteger(num);
            BigInteger id;
            String value = new String();
            id = keyInteger.mod(Bignum);
            int SelectedId = id.intValue();
            int SelectedPort = Integer.parseInt(this.IDextracter.get_port_number(this.ServerList.get(SelectedId)));
            String SelectedIP = this.IDextracter.get_ip_string(this.ServerList.get(SelectedId));
            String serverID = this.ServerList.get(SelectedId);
            if (SocketList.get(serverID) == null) {
                Socket PeertoServerSocket = new Socket(SelectedIP, SelectedPort);
                SocketList.put(serverID, PeertoServerSocket);

                ObjectOutputStream out = new ObjectOutputStream(PeertoServerSocket.getOutputStream());
                outList.put(serverID, out);
                ObjectInputStream in = new ObjectInputStream(PeertoServerSocket.getInputStream());
                inList.put(serverID, in);
                MessageCommand msg = new MessageCommand(Command.DELETE, key, value);
                out.writeObject(msg);//sending the delete message to the server
                out.flush();
                msg = (MessageCommand) in.readObject();//receiving the  message from the server

            } else {

                MessageCommand msg = new MessageCommand(Command.DELETE, key, value);
                outList.get(serverID).writeObject(msg);//sending the delete message to the server
                outList.get(serverID).flush();
                msg = (MessageCommand) inList.get(serverID).readObject();//receiving the  message from the server
            }
            return true;

        } catch (IOException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
public String randomString(int len) {

	char[]a= new char[len];
         
	 String alphanum= "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	for (int i = 0; i < len; ++i) {
            Random rand=new Random();
          int n = (rand.nextInt(62));
		a[i]=alphanum.charAt(n);
	}
        String s=a.toString();
	return s;
}
    /**
     * implement the TEST operation to respectively test 100K put requests, 100K
     * get requests and 100K delete requests
     */
    public void test(int num) {
        System.out.println("Start performance test, please be patient ...");
       // int num = 100000;
        boolean flag;
        //String value2 = "CS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aaCS550PA2aa";
        long startTime, endTime;
        startTime = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String key2 = randomString(10);
            String value2 =randomString(90);
            flag = put(key2, value2);
        }
        endTime = System.currentTimeMillis();
        System.out.println("put test takes: " + (double) (endTime - startTime) / num * 1000 + " us ");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String key2 = randomString(10);
            String value = get(key2);
        }
        endTime = System.currentTimeMillis();
        System.out.println("get test takes: " + (double) (endTime - startTime) / num * 1000 + " us ");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            String key2 = randomString(10);
            flag = delete(key2);
        }
        endTime = System.currentTimeMillis();
        System.out.println("delete test takes: " + (double) (endTime - startTime) / num * 1000 + " us ");
    }

    /**
     *
     * @param args
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        // TODO code application logic here
        String key = new String();
        String value = new String();
        System.out.println("********************************************");
        System.out.println("*         Peer Operation Command           *");
        System.out.println("*                                          *");
        System.out.println("* 1.PUT      (upload the key and value)    *");
        System.out.println("* 2.GET      (download the value)          *");
        System.out.println("* 3.DELETE   (delete the key)              *");
        System.out.println("* 4.TEST     (test the system performance) *");
        System.out.println("* 5.EXIT     (exit the peer)               *");
        System.out.println("********************************************");
        Parser IDextracter = new Parser();
        ArrayList<String> ServerList = IDextracter.configure();//storing the server ip and port being deploied
        int ServerNum = IDextracter.getNumItems(ServerList);
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));       
        int portnumber=IDextracter.getserverid(ServerNum);
        int port = Integer.parseInt(IDextracter.get_port_number(ServerList.get(portnumber-1)));
        DistributedHashTable Server = new DistributedHashTable(port);
        System.out.println("Input the command: ");
        String fromUser;

        while ((fromUser = stdIn.readLine()) != null) {

            if (fromUser.equalsIgnoreCase("PUT")) {
                try {
                    System.out.println("key: ");

                    key = stdIn.readLine();

                    System.out.println("value: ");
                    value = stdIn.readLine();
                    long startTime, endTime;
                    boolean flag = Server.put(key, value);
                    // put operation

                } catch (IOException ex) {
                    Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
                }

                System.out.println("Input the command: ");
            } else if (fromUser.equalsIgnoreCase("GET")) {

                try {
                    System.out.println("key: ");
                    key = stdIn.readLine();
                    value = Server.get(key);
                    // get operation
                    if (value == null) {
                        System.out.println("the key doesn't exist!");
                    } else {
                        System.out.println("the value is " + value);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
                }

                System.out.println("Input the command: ");
            } else if (fromUser.equalsIgnoreCase("DELETE")) {

                try {
                    System.out.println("key: ");

                    key = stdIn.readLine();

                    boolean flag1 = Server.delete(key);//delete operation
                } catch (IOException ex) {
                    Logger.getLogger(DistributedHashTable.class.getName()).log(Level.SEVERE, null, ex);
                }

                System.out.println("Input the command: ");
            } else if (fromUser.equalsIgnoreCase("TEST")) {
                System.out.println("Number of requests: ");
                int num = Integer.parseInt(stdIn.readLine());
                Server.test(num);

                System.out.println("Input the command: ");
            } else if (fromUser.equalsIgnoreCase("EXIT")) {
                int i;

                for (i = 0; i < Server.ServerNum; i++) {//release the socket connection
                    ObjectInputStream tmp2 = Server.inList.get(Server.ServerList.get(i));
                    if (tmp2 != null) {
                        tmp2.close();
                    }
                    ObjectOutputStream tmp1 = Server.outList.get(Server.ServerList.get(i));
                    if (tmp1 != null) {
                        tmp1.close();
                    }
                    Socket tmp = Server.SocketList.get(Server.ServerList.get(i));
                    if (tmp != null) {
                        tmp.close();
                    }
                }
                stdIn.close();
                Server.PeerServer.close();

                break;
            } else {
                System.out.println("Only LOOKUP, DOWNLOAD ,TEST and EXIT suppport, please try again");
                System.out.println("Input the command: ");
            }
        }
    }

}

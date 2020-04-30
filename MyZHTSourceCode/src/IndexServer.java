/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distributed;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author zn
 */
public class IndexServer implements Runnable {

    private Socket ClientSocket;
    private static Map<String, String> fileMap = new ConcurrentHashMap<String, String>();

    public IndexServer(Socket connection) {

        this.ClientSocket = connection;
    }

    public void run() {

//        BufferedWriter output = null;
//        BufferedReader input = null;
        try {

            String key = new String();
            String value = new String();
            ObjectInputStream in = new ObjectInputStream(this.ClientSocket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(this.ClientSocket.getOutputStream());
            //output = new BufferedWriter(new OutputStreamWriter(ClientSocket.getOutputStream()));
            //input = new BufferedReader(new InputStreamReader(ClientSocket.getInputStream()));
            MessageCommand message = new MessageCommand(Command.OK, key, value);
            Command cmd;
            String inputLine;
            // Gson gson = new Gson();
            while (this.ClientSocket.isConnected()) {
               // if ((inputLine = input.readLine()) != null) {
                //System.out.println(inputLine);
                // message = gson.fromJson(inputLine, MessageCommand.class);
                
                message=(MessageCommand)in.readObject();

                cmd = message.getCmd();

                switch (cmd) {
                    case PUT:
                        //   System.out.println("Put from " + ClientSocket.getRemoteSocketAddress());
                        key = (String) message.getKeyBody();
                        value = (String) message.getValueBody();
                        fileMap.put(key, value);
                         message.setCmd(Command.OK);
                            message.setKeyBody(key);
                            message.setValueBody(value);
                            out.writeObject(message);
                            out.flush();
                        // System.out.println(key+"  "+value);
                        break;
                    case GET:
                        //System.out.println("Get from " + ClientSocket.getRemoteSocketAddress());
                        String fileSearched = (String) message.getKeyBody();
                        value = fileMap.get(fileSearched);

                       // System.out.println(value);
                        if (value != null) { //sent query result back
                            message.setCmd(Command.RESULT);
                            message.setKeyBody(fileSearched);
                            message.setValueBody(value);
                            out.writeObject(message);
                            out.flush();

                        } else {
                            value = "EMPTY";
                            message.setCmd(Command.EMPTY);
                            message.setKeyBody(fileSearched);
                            message.setValueBody(value);
                             out.writeObject(message);
                            out.flush();
                        }
                        break;
                    case DELETE:
                        // System.out.println("Delete from " + ClientSocket.getRemoteSocketAddress());
                        key = (String) message.getKeyBody();
                        fileMap.remove(key);
                         message.setCmd(Command.OK);          
                            out.writeObject(message);
                            out.flush();
                        // System.out.println(key);
                        break;
                }
            }
            //}

        } catch (IOException ex) {
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(IndexServer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
               // output.close();
                //
//                input.close();
                ClientSocket.close();
            } catch (IOException ex) {
                System.out.println("Close failed!");
            }
        }
    }
}

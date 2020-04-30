/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distributed;

import java.io.Serializable;

/**
 *
 * @author zn
 */

public class MessageCommand implements Serializable{
    
    public Command cmd;
    public String keybody;
    public String valuebody;

 

    public MessageCommand(Command cmd, String keybody, String valuebody) {
        this.cmd = cmd;
        this.keybody = keybody;
        this.valuebody = valuebody;
    }

    public Command getCmd() {
        return cmd;
    }

    public void setCmd(Command cmd) {
        this.cmd = cmd;
    }

    public Object getKeyBody() {
        return keybody;
    }

    public void setKeyBody(String body) {
        this.keybody = body;
    }  
    public Object getValueBody() {
        return valuebody;
    }
     public void setValueBody(String body) {
        this.valuebody = body;
    } 
}

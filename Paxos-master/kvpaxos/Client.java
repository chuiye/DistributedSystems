package kvpaxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class Client {
    String[] servers;
    int[] ports;

    // Your data here
    int seq;

    public Client(String[] servers, int[] ports){
        this.servers = servers;
        this.ports = ports;
        // Your initialization code here
        this.seq = -1;
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;
        KVPaxosRMI stub;
        try{
            Registry registry= LocateRegistry.getRegistry(this.ports[id]);
            stub=(KVPaxosRMI) registry.lookup("KVPaxos");
            if(rmi.equals("Get"))
                callReply = stub.Get(req);
            else if(rmi.equals("Put")){
                callReply = stub.Put(req);}
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    public Integer Get(String key){
        // Your code here
        Request request = new Request(new Op("Get", this.seq, key, null));
        Response reply;
        while (true) {
            for (int i = 0; i < this.ports.length; i++) {
                reply = Call("Get", request, i);
		//System.out.println("succ: " + reply.succ + " i: " + i);
                if (reply.succ) return reply.op.value;
            }
        }
        //return -1;
    }

    public boolean Put(String key, Integer value){
        // Your code here
	this.seq++;
        Request request = new Request(new Op("Put", this.seq, key, value));
        Response reply;
        while (true) {
            for (int i = 0; i < this.ports.length; i++) {
                reply = Call("Put", request, i);
		//System.out.println("succ: " + reply.succ + " i: " + i);
                if (reply.succ) return true;
            }            
        }
        //return false;
    }

}

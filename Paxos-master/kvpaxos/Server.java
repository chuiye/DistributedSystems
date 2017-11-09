package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    Map<String, Integer> map;


    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        this.map = new HashMap<>();

        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    // RMI handlers
    public Response Get(Request req){
        Map<String, Integer> value = wait(req.op.ClientSeq);
        if (value != null && value.containsKey(req.op.key)) {
            return new Response(true, new Op("Get", req.op.ClientSeq, null, value.get(req.op.key)));
        } 
        return new Response(false, new Op("Get", -1, null, null));
    }

    public Response Put(Request req){
        this.map.put(req.op.key, req.op.value);
        this.px.Start(req.op.ClientSeq, this.map);
        return new Response(true, new Op("Put", req.op.ClientSeq, null, null));
    }

    private Map<String, Integer> wait(int seq) {
        int to = 10;
        while(true) {
            Paxos.retStatus ret = this.px.Status(seq);
	    //System.out.println("status: "+ ret.state); 
            if (ret.state == State.Decided) {
                return (Map<String, Integer>)ret.v;
            }
            try {
                Thread.sleep(to);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (to < 1000) {
                to = to * 2;
            }
        }
        //return null;
    }

}

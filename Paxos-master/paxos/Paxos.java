package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import java.util.*;
import java.lang.*;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    int seq;
    Object value;
    Map<Integer, Object> chosenValue;

    int minProposal, acceptedProposal;
    Object acceptedValue;

    int[] minDoneSeqs; // non-local, except doneSeqs[me]

    int maxSeqSeen;

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
	this.seq = -1;
	this.value = null;
	this.chosenValue = new HashMap<>();
	this.minProposal = -1;
	this.acceptedProposal = -1;
	this.acceptedValue = null;
	this.minDoneSeqs = new int[peers.length];
	Arrays.fill(this.minDoneSeqs, -1);
	this.maxSeqSeen = -1;

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
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

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,

     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        if (seq < this.Min()){
            return;
        }

        this.seq = seq;
        this.value = value;
	
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run(){
        //Your code here
       // ignore when the instance seq is less than or equal to minDoneSeq 
/*
        if (this.seq <= this.minDoneSeq){
            return;
        }
*/
	int curSeq;
	Object curValue;
        mutex.lock();
        try {
	    curSeq = this.seq;
            curValue = this.value;
            this.maxSeqSeen = Math.max(this.maxSeqSeen, this.seq);
        } finally {
            mutex.unlock();
        }

//        mutex.lock();
//        try {
            int peers_num = this.peers.length;
            int majority_num = peers_num / 2 + 1;

            //propose_num = this.me;
	    int propose_num = this.me;

            while (!this.isDead()) {
                int next_propose_num = propose_num;

                //send prepare(n)
                Request prepareRequest = new Request("Prepare", curSeq, propose_num, null);                           
                List<Response> prepareReplies = new ArrayList<>();
                for (int i = 0; i < peers_num; i++) {             
                    Response prepareReply;

                    if (i != this.me) {
                        prepareReply = Call("Prepare", prepareRequest, i);                        
                    } else {
                        prepareReply = this.Prepare(prepareRequest);
                    }

                    if (prepareReply != null) {
                        this.maxSeqSeen = Math.max(this.maxSeqSeen, prepareReply.seq);
                        if (prepareReply.succ) {
                            prepareReplies.add(prepareReply);
                        } else if (prepareReply.N > next_propose_num) {
                            next_propose_num = prepareReply.N;
                        }
                    }
                }

                //TODO: barrier here
                int accepted_propose_num = -1;
                Object accepted_value = null;
                if (prepareReplies.size() >= majority_num) {
                    for (Response r: prepareReplies) {
                        if (r.N_a > accepted_propose_num) {
                            accepted_propose_num = r.N_a;
                            accepted_value = r.V_a;
                        }
                    }
                }

                // send accept(n, value)
                List<Response> acceptReplies = new ArrayList<>();
                Object accept_req_value;
                if (accepted_value != null) {
                    accept_req_value = accepted_value;
                } else {
                    accept_req_value = curValue;
                }

                Request acceptRequest = new Request("Accept", curSeq, propose_num, accept_req_value);             
                for (int i = 0; i < peers_num; i++) {             
                    Response acceptReply;

                    if (i != this.me) {
                        acceptReply = Call("Accept", acceptRequest, i);                        
                    } else {
                        acceptReply = this.Accept(acceptRequest);
                    }

                    if (acceptReply != null) {
                        this.maxSeqSeen = Math.max(this.maxSeqSeen, acceptReply.seq);
                        if (acceptReply.succ) {
                            acceptReplies.add(acceptReply);
                        } else if (acceptReply.N > next_propose_num) {
                            next_propose_num = acceptReply.N;
                        }
                    }
                }

                //TODO: barrier here

                //send decide(v) to all
                if (acceptReplies.size() >= majority_num) {
                    Request decideRequest = new Request("Decide", curSeq, -1, accept_req_value); //null -> -1

                    Response decideReply;
                    for (int i = 0; i < peers_num; i++) {             
                        if (i != this.me) {
                            decideReply = Call("Decide", decideRequest, i);
			    if (decideReply != null) { 
			        mutex.lock();
			        try {
			   	    if (decideReply.seq > this.minDoneSeqs[i]) {
					this.minDoneSeqs[i] = decideReply.seq;
				    }
			        } finally {
				    mutex.unlock();
			        }
			    }
                        } else {
                            decideReply = this.Decide(decideRequest);
                        }
                    }
                    break;
                }

                int try_num = next_propose_num/peers_num*peers_num + this.me;
                if (try_num > next_propose_num) {
                    next_propose_num = try_num;
                } else {
                    next_propose_num = try_num + peers_num;
                }
                
                // if (next_propose_num <= propose_num || next_propose_num%peers_num != proposer.mgr.me) {
                //     log.Fatalln("unexpected error!!!")
                // }
                propose_num = next_propose_num;

                // sleep for avoiding thrashing of proposing
/*		try{
                	Thread.sleep(50);
		}catch(Exception e){
			e.printStackTrace();		
		}
*/
            }

//        } finally {
//            mutex.unlock();
//        }
    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        mutex.lock();
        try {
            Response response;
            if (req.N > minProposal) {
                minProposal = req.N;
                response = new Response("Prepare", -1, acceptedProposal, acceptedValue, true, this.minDoneSeqs[this.me]); // null->-1
            } else {
               response = new Response("Prepare", -1, acceptedProposal, acceptedValue, false, this.minDoneSeqs[this.me]); // null->-1
            }
            return response;
        } finally {
            mutex.unlock();
        }
    }

    public Response Accept(Request req){
        // your code here
        mutex.lock();
        try {
            Response response;
            if (req.N >= minProposal) {
                acceptedProposal = req.N;
                acceptedValue = req.V;
                minProposal = req.N;
                response = new Response("Accept", req.N, -1, null, true, this.minDoneSeqs[this.me]); //null->-1
            } else {
               response = new Response("Accept", minProposal, -1, null, false, this.minDoneSeqs[this.me]);//null->-1 
            }
            return response;
        } finally {
            mutex.unlock();
        }

    }

    public Response Decide(Request req){
        // your code here
        mutex.lock();
        try {
            this.chosenValue.put(req.seq, req.V);
	    //System.out.println("Decided thread: " + this.me);
            return new Response("Decide", -1, -1, null, true, this.minDoneSeqs[this.me]); //null->-1
        } finally {
            mutex.unlock();
        }        

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        mutex.lock();
        try {
            if (seq > this.minDoneSeqs[this.me])
                this.minDoneSeqs[this.me] = seq;
        } finally {
            mutex.unlock();
        }

    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        mutex.lock();
        try {
            return this.maxSeqSeen;            
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().
     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.
     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.
     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        mutex.lock();
        try {	
		int minSeqDone = Integer.MAX_VALUE;
		for (int j = 0; j < this.minDoneSeqs.length; j++) {
			if (minSeqDone > this.minDoneSeqs[j]) {
				minSeqDone = this.minDoneSeqs[j];
			}
		}	
		return minSeqDone + 1;
        } finally {
            mutex.unlock();
        }
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. status()
     * should just inspect the local peer state;
     * it should not contact other paxos peers.
     */
    public retStatus Status(int seq){
        if (seq < this.Min()) {
		//system.out.println("forgotten min():" + this.min());
            return new retStatus(State.Forgotten, null);
        }
        mutex.lock();
        try {
            if (this.chosenValue.containsKey(seq)) {
                return new retStatus(State.Decided, this.chosenValue.get(seq));
            } else {
                return new retStatus(State.Pending, null);
            }           
        } finally {
            mutex.unlock();
        }        

    }

    /**
     * helper class for status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * tell the peer to shut itself down.
     * for testing.
     * please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("none reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isUnreliable(){
        return this.unreliable.get();
    }


}

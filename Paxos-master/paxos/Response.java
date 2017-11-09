package paxos;
import java.io.Serializable;

import java.util.*;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    int N;
    int N_a;
    Object V_a;
    boolean succ;

    String rmi;
    int seq;

    // Your constructor and methods here
//    public Response(String rmi, int N, int N_a, Map<String, Integer> V_a, boolean succ, int seq) {
    public Response(String rmi, int N, int N_a, Object V_a, boolean succ, int seq) {
    	this.N = N;
    	this.N_a = N_a;
    	this.V_a = V_a;
    	this.succ = succ;
    	this.rmi = rmi;
    	this.seq = seq;
    }
}

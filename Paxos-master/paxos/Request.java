package paxos;
import java.io.Serializable;

import java.util.*;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: You may need the sequence number for each paxos instance and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: Easier to make each variable public
 */
public class Request implements Serializable {
    static final long serialVersionUID=1L;
    public int seq;
    public int N;
    public Object V;

    public String rmi;

    // Your constructor and methods here
//    public Request(String rmi, int seq, int N, Map<String, Integer> V) {
    public Request(String rmi, int seq, int N, Object V) {
    	this.seq = seq;
    	this.N = N;
    	this.V = V;
    	this.rmi = rmi;
    }
}

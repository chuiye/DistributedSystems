package chuiye;

import java.io.Serializable;

public class Line implements Serializable {
    String stockName;
    String date;
    long val;
    
    public Line(String name, String date, long val) {
        this.stockName = name;
        this.date = date;
        this.val = val;
    }
}

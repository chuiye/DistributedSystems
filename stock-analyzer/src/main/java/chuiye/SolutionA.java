package chuiye;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.col;



import java.util.*;
import java.io.*;

public class SolutionA {
    public void run() {
        String path = "/home/ckong/ds/hw4/stock-analyzer/src/resources/NASDAQ100"; // stock file directory
        List<String> filesList = new ArrayList<String>();
        File[] files = new File(path).listFiles();
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".csv")) {
                filesList.add(file.getName());
            }
        }

        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Collections.sort(filesList);

        try (FileWriter f = new FileWriter("output_1.txt")){
            for (String fileName: filesList) {
                Dataset<Row> logData = spark.read().option("header", "true").csv(path + "/"  + fileName).select("Open", "Close");
                long num = logData.filter(((col("Close").minus(col("Open"))).divide(col("Open"))).gt(0.01)).count();
                //System.out.println(fileName + "," + num);
                f.write(fileName.substring(0, fileName.lastIndexOf('.')) + "," + num + "\n");
            }
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        spark.stop();
    }

}




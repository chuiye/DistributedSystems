package chuiye;

import scala.Tuple2; 

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.*; 
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.api.java.function.PairFunction; 
 
import java.util.*; 
import java.io.*;

public class SolutionB {
    public void run() {
       String path = "/home/ckong/ds/hw4/stock-analyzer/src/resources/NASDAQ100/"; // stock file directory
        List<String> filesList = new ArrayList<String>();
        File[] files = new File(path).listFiles();
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".csv")) {
                filesList.add(file.getName());
            }
        }

        JavaSparkContext sc = new JavaSparkContext("local[4]", "App");

        JavaPairRDD<String, Tuple2<Double, String>> pairs = JavaPairRDD.fromJavaRDD(sc.emptyRDD());
        for (String fileName: filesList) {
            //System.out.println(path + fileName);
            JavaRDD<String> allRows = sc.textFile(path + fileName);
            String header = allRows.first();//take out header
            JavaRDD<String> filteredRows = allRows
                .filter(row -> !row.equals(header))
                .map(row -> fileName.substring(0, fileName.lastIndexOf(".")) + "," + row);//filter header

            JavaPairRDD<String, Tuple2<Double, String>> filteredRowsPairRDD = filteredRows.mapToPair(parseCSVFile);//create pair

            pairs = sc.union(pairs, filteredRowsPairRDD);
        }

        JavaPairRDD<String, Iterable<Tuple2<Double, String>>> groups = pairs.sortByKey().groupByKey(); 
        JavaPairRDD<String, Iterable<Tuple2<Double, String>>> sorted = groups.mapValues(sortByValue); 

        List<Tuple2<String, Iterable<Tuple2<Double, String>>>> output = sorted.collect();

        try (FileWriter f = new FileWriter("output_2.txt")){
            for (Tuple2<String, Iterable<Tuple2<Double, String>>> t : output) { 
                f.write(t._1 + ",[");
                Iterable<Tuple2<Double, String>> list = t._2;
                int i = 0;  
                for (Tuple2<Double, String> item: list) { 
                    if (i == 5) break;
                    f.write(item._2);
                    if (i != 4) {
                        f.write(",");
                    }
                    i++;
                }
                f.write("]\n");
            }
            f.close();
        } catch (IOException e) {
            e.printStackTrace();
        } 

 
        
/*        List<Tuple2<String, Tuple2<Double, String>>> output = groups.collect(); 
        for (Tuple2 t : output) { 
           Tuple2<Double, String> valuename = (Tuple2<Double, String>) t._2; 
           System.out.println(t._1 + "," + valuename._1 + "," + valuename._2); 
        }
*/ 
        sc.stop(); 
    } 
    
    private static PairFunction<String, String, Tuple2<Double, String>> parseCSVFile = (row) -> {
        String[] fields = row.split(",");
        double open = Double.parseDouble(fields[2]);
        double close = Double.parseDouble(fields[5]);
        return new Tuple2<String, Tuple2<Double, String>>(fields[1], new Tuple2((close - open) / open, fields[0]));
    };

    private static Function<Iterable<Tuple2<Double, String>>, Iterable<Tuple2<Double, String>>> sortByValue = (iter) -> {
        List<Tuple2<Double, String>> newList = new ArrayList<Tuple2<Double, String>>(iterableToList(iter));       
        Collections.sort(newList, new Comparator<Tuple2<Double, String>>(){
            @Override
            public int compare(Tuple2<Double, String> a, Tuple2<Double, String> b) {
                 if (a._1 == b._1) return 0;
                 else if (a._1 > b._1) return -1;
                 else return 1;
            }
        });   
        return newList;         
    };      

    private static List<Tuple2<Double, String>> iterableToList(Iterable<Tuple2<Double, String>> iterable) { 
        List<Tuple2<Double, String>> list = new ArrayList<Tuple2<Double, String>>(); 
        for (Tuple2<Double, String> item : iterable) { 
            list.add(item); 
        } 
        return list; 
    } 
  
}             
             
             

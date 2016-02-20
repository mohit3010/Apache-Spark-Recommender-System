package org.apache.spark.rdd;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.types.DateType;

import akka.util.Collections;
import antlr.collections.List;
import scala.Tuple2;
import scala.collection.immutable.Seq;

public class TimeRecommend {
	
	public static void main(String[] args) throws ParseException {
		
		SparkConf config = new SparkConf().setMaster("local").setAppName("TimeRecommend");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		
		JavaRDD<String>  logFilesRDD = sc.textFile("Data/SandeshLog/*");
		
		//PairRdd111.coalesce(1).saveAsTextFile("Data/SandeshOutput1");
		
		JavaRDD<String> filteredRDD = logFilesRDD.filter(new Function<String, Boolean>() {
			
			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return (!arg0.startsWith("#"));
			}
		});
		
		//filteredRDD.coalesce(1).saveAsTextFile("Data/SandeshOutput");
		
		
		//----------- Making Date Format as values ------------
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JavaPairRDD<String, String> ipTimePairRDD = filteredRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1]),arg0.split(" ")[10]);
			}
		});
		
		
//		System.out.println("ipTimePairRDD Count :"+ipTimePairRDD.count());
//		System.out.println("First 10 ipTimePairRDD : "+ipTimePairRDD.take(10));	
		
		
		//----------- Dates grouped in Ascending order by userIP-------------------------------------
		JavaPairRDD<String, String> intermediateRDD1= ipTimePairRDD.sortByKey();
		JavaPairRDD<String, String> intermateRDD2= intermediateRDD1.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._2(),arg0._1());
			}
		});

		JavaPairRDD<String, Iterable<String>> perIPDateRDD = intermateRDD2.groupByKey();
		System.out.println("perIPDateRDD count : "+perIPDateRDD.count()+"First 1 : "+perIPDateRDD.take(1));
		  
		String[] s = perIPDateRDD.values().toString().split(",");
		for(int i = 0 ; i<s.length;i++)
		{
			
			s[i].replace("[", "");
			s[i].replace("]", "");
			
			
		}

		ArrayList<Date> dateArrayList = new ArrayList<Date>();
		for(String s1 : s)
		{
			
			Date d = df.parse(s1);
			dateArrayList.add(d);
			
		}
		
		
		System.out.println(dateArrayList.get(1));
		
	}

}
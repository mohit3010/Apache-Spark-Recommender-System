package org.apache.spark.rdd;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import scala.Tuple2;

public class GtplUsage {
	private static Integer usage;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GtplUsage");
		JavaSparkContext sc = new JavaSparkContext(conf);
		long start = System.currentTimeMillis();
		JavaPairRDD<String, Integer> DateUsageRDD = sc.textFile("Data/GtplUsage.csv").mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				if(arg0.split(",")[1].contains(" "))
					return new Tuple2(arg0.split(",")[1].split(" ")[0], Integer.parseInt(arg0.split(",")[3]));
				return new Tuple2(arg0.split(",")[1], Integer.parseInt(arg0.split(",")[3]));
			}
			
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		}).sortByKey(false).filter(new Function<Tuple2<String,Integer>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2()>=1024;
			}
		});
		JavaRDD<Integer> Usage = DateUsageRDD.values();
		Integer i = Usage.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
		//DateUsageRDD.coalesce(1).saveAsTextFile("Data/GtplUsageFiltered102");
		System.out.println("Usage :"+i/1024);
		long stop = System.currentTimeMillis();
		System.out.println(stop-start);
		sc.close();
	}
		
	
}

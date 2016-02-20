package org.apache.spark.rdd;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.apache.flume.annotations.InterfaceAudience.Public;
import org.apache.hadoop.fs.Hdfs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.codehaus.janino.Java;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;



public class rdd1 {
	public static void main(String[] args) {
			
//						Configuration
		SparkConf config = new SparkConf().setMaster("local").setAppName("rdd1");
		JavaSparkContext sc = new JavaSparkContext(config);
		
//						InputRDD from log1.txt
		JavaRDD<String> inputRDD = sc.textFile("Data/Log1.txt");
		
		
		
//					 new RDD that Contains "error"
		JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
			
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("warning");
			}
		});
		
		
//					new RDD contains "warning
		JavaRDD<String> warningRDD = inputRDD.filter(new Function<String, Boolean>() {
			public Boolean call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.contains("error");
			}
		});
		
	
//					 RDD that cotains both "warning"+"error"
		JavaRDD<String> badLinesRDD = errorRDD.union(warningRDD);
		//System.out.println("System have "+badLinesRDD +"bad lines");
		//System.out.println("Some of them are as follows:");
		for(String line: badLinesRDD.take(15))
		{
			System.out.println(line);
		}
		
		
		
//					 map example by squaring elements
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			public Integer call(Integer x) throws Exception {
				// TODO Auto-generated method stub
				return x*x;
			}
		});
		
		//System.out.println(result.collect());
	
		
//					FlatMap example by splitting words
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("hello I am niyat", "i work here", "ganhinagar", "gujarat"));
		JavaRDD<String> words = rdd1.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(x.split(" "));
			}
		});
	
			//System.out.println(words.collect());
		//System.out.println(words.first());
		
//						 creating Pari-RDD from InputRDD
		
		JavaPairRDD<String, String> inputPairRDD = inputRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(x.split(" ")[0],x);
			}
		}); 	
		System.out.println("inputPariRDD :"+inputPairRDD.take(5));
		
		
//						Filter words less than 20 characters
		JavaPairRDD<String, String> rdd2 = inputPairRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			
			public Boolean call(Tuple2<String, String> x) throws Exception {
				// TODO Auto-generated method stub
				return x._2().length()<20;
			}
		});
		System.out.println("inputPairRDD with character<20 :" +rdd2.take(5));
		String[] s = new String[4000];
		
		
		
//								Wordcount
		JavaRDD<String> inputRDDwords = inputRDD.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> inputPairRDD1 = inputRDDwords.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(x, 1);
			}
		}).reduceByKey((x,y) -> x+y);/* reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});*/
		System.out.println("inputPairRDD1 :"+inputPairRDD1.take(5));
		
		
		

		sc.close();
		
		
		
	}
}
		
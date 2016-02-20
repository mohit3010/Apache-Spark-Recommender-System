package org.apache.spark.rdd;

import org.apache.spark.SparkConf;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.catalyst.expressions.In;

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;

public class TopSearchByUser {;
	public static void main(String[] args) {
		SparkConf config = new SparkConf().setMaster("local").setAppName("TopSearchbyUser");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<String> f2fLogRDD = sc.textFile("Data/Fibre2Fashion/F2F.log").distinct();
		
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JavaPairRDD<String, String> f2fPairRDD = f2fLogRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new Tuple2(df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1]),arg0.split(" ")[9]+"-"+df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1])+","+arg0.split(" ")[6]);
				
				
			}
		}).sortByKey();
		
		JavaPairRDD<String,String>IPPairRDD = f2fPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(arg0._2().split("-")[0],arg0._2().split("-")[1]);
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {
			
			public Boolean call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2().contains("section=leads"); //You might think 'return' statement as Condition specifier!
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				//return new Tuple2(arg0._1(), arg0._2().split(",")[0]+":"+arg0._2().split(",")[1].split("&")[0].replaceAll("q=",""));
				if(arg0._2().split(",")[1].startsWith("q="))
					return new Tuple2(arg0._1(), arg0._2().split(",")[0]+"-"+arg0._2().split(",")[1].split("&")[0].replaceAll("q=","").replace("+", " ").replaceAll("%20", " ").replaceAll("%2", " ").replaceAll("%", " "));
				//return new Tuple2(arg0._1(), arg0._2().split(",")[0]+":"+arg0._2().split(",")[1].substring(arg0._2().indexOf("q")+2,arg0._2().indexOf("&")));
				else //if(arg0._2().startsWith("section="))
					return new Tuple2(arg0._1(), arg0._2().split(",")[0]+"-"+arg0._2().split(",")[1].split("&")[1].replaceAll("q=","").replace("+", " ").replaceAll("%20", " ").replaceAll("%2", " ").replaceAll("%", " "));
				//return new Tuple2(arg0._1(), arg0._2().split(",")[0]+":"+arg0._2().split(",")[1].substring(arg0._2().indexOf("q")+2,arg0._2().indexOf("&")))
						
			}
		});
		
		JavaPairRDD<String, String> termsbyUser=IPPairRDD.mapToPair(new PairFunction<Tuple2<String,String>,String, String>() {

			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(), arg0._2().split("-")[1]/*.split(" ")*/);
			}
		});
		
		//termsbyUser.coalesce(1).saveAsTextFile("Data/F2FOutput/outputByUser");
		
		
		JavaPairRDD<String, String> singleTermByUserPairRDD = termsbyUser.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
		@Override
		public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
			// TODO Auto-generated method stub
			for(int i=0;i<=arg0._2().split(" ").length;i++)
			{
			return new Tuple2(arg0._1, arg0._2().split(" ")[i]);
			}
			return arg0;
		}
		});
		
		singleTermByUserPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/singleTermByUSerPairRDD");
		
		
		
		
		JavaPairRDD<String, Integer> rddp1 = singleTermByUserPairRDD.mapValues(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0+" 1";
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1()+" "+arg0._2().split(" ")[0],Integer.parseInt(arg0._2().split(" ")[1]) );
			}
		})/*.groupByKey().mapValues(new Function<Iterable<String>, String>() {
			@Override
			public String call(Iterable<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				
			}
			
		})*/;
		
		
		JavaPairRDD<String, Integer> rddp3 = rddp1.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				// TODO Auto-generated method stub
				return x+y;
			}
		});
		JavaPairRDD<String, Iterable<String>> rdd10 = rddp3.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(arg0._2(), arg0._1());
			}
		}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<Integer, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._2().split(" ")[0], arg0._2().split(" ")[1]+"-"+arg0._1());
			}
		}).groupByKey();
		//rdd10.coalesce(1).saveAsTextFile("Data/F2FOutput/100");
		
		/*JavaPairRDD<String, String> rdd7 = rddp3.mapToPair(new PairFunction<Tuple2<String,Integer>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1().split(" ")[0], arg0._1().split(" ")[1]+"-"+arg0._2());
			}
			
		});
		
		rdd7.coalesce(1).saveAsTextFile("Data/F2FOutput/40");*/
		
		/*
		//System.out.println("singleTermByUserPairRDD Count:"+singleTermByUserPairRDD.count()+"First 5:"+singleTermByUserPairRDD.take(5));
		JavaPairRDD<String, Tuple2<String, Integer>> rddn1 = singleTermByUserPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(), new Tuple2(arg0._2(), 1));
			}
		});
		//System.out.println(rddn1.count());
		//System.out.println(rddn1.take(5));
		
		JavaPairRDD<String, Tuple2<String, Integer>> rddn2 = rddn1.reduceByKey(new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
			
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> arg0, Tuple2<String, Integer> arg1) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(),arg1._1()+arg1._2() );
			}
		});
		System.out.println(rddn2.count());
		rddn2.coalesce(1).saveAsTextFile("Data/F2FOutput/1");*/
	}
}

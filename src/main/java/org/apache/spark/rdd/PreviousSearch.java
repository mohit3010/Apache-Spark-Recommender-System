package org.apache.spark.rdd;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.linq4j.tree.LambdaExpression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.EndsWith;


import breeze.math.MutablizingAdaptor.Lambda2;
import scala.Tuple2;

public class PreviousSearch {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("PreviousSearch");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//----------Adding f2f Log files----------------
		//sc.addFile("Data/Fibre2Fashion/F2F.log");
		JavaRDD<String> f2fLogRDD = sc.textFile("Data/Fibre2Fashion/F2F.log").distinct();
		
		
		
		
		//Extracting IP, Time and searchQuery fields from f2fLog RDD
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JavaPairRDD<String, String> f2fPairRDD = f2fLogRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new Tuple2(df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1]),arg0.split(" ")[9]+"-"+df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1])+","+arg0.split(" ")[6]);
				
				
			}
		}).sortByKey();
		JavaPairRDD<String, String> groupedByIPPairRDD = f2fPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
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
					return new Tuple2(arg0._1(), arg0._2().split(",")[0]+"-"+arg0._2().split(",")[1].split("&")[0].replaceAll("q=","").replace("+", " ").replaceAll("%20", " ").replaceAll("%2", " "));
				//return new Tuple2(arg0._1(), arg0._2().split(",")[0]+":"+arg0._2().split(",")[1].substring(arg0._2().indexOf("q")+2,arg0._2().indexOf("&")));
				else //if(arg0._2().startsWith("section="))
					return new Tuple2(arg0._1(), arg0._2().split(",")[0]+"-"+arg0._2().split(",")[1].split("&")[1].replaceAll("q=","").replace("+", " ").replaceAll("%20", " ").replaceAll("%2", " "));
				//return new Tuple2(arg0._1(), arg0._2().split(",")[0]+":"+arg0._2().split(",")[1].substring(arg0._2().indexOf("q")+2,arg0._2().indexOf("&")))
						
			}
		});
		//groupedByIPPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/5");
		
		
		//-------------Term Frequency-------------------
		JavaRDD<String> rdd3 = groupedByIPPairRDD.values().flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split("-")[1].split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(new PairFunction<String, String, Integer>() {
		public Tuple2<String, Integer> call(String arg0) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, Integer>(arg0, 1);
		}
		});
		
		JavaPairRDD<String, Integer> interMedRDD5= rdd4.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._2(), arg0._1());
			}
		}).sortByKey(false).mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(), arg0._2());
			}
		});
		
		List<Tuple2<String, Integer>> allrecordstopSearch=interMedRDD5.toArray();
	
		List<Tuple2<String, Integer>> firstfiverecord=allrecordstopSearch.subList(0, 5);
		
		JavaRDD<Tuple2<String, Integer>> topfiveSearhRDD=sc.parallelize(firstfiverecord);
		topfiveSearhRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/top5");	
		//System.out.println(rdd3.count()+"1:"+rdd3.take(5));
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
//		JavaPairRDD<String, String> rdd2 = groupedByIPPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
//		public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
//			// TODO Auto-generated method stub
//			return new Tuple2(arg0._2().split("-")[0], arg0);
//		}
//		}).sortByKey();
		
//		groupedByIPPairRDD.coalesce(1).saveAsTextFile("Data/Fibre2Fashion_output16");
		
		//---------------Finding TermFrequency-----------------------------
		
		
//		JavaPairRDD<String, String> rdd3 = rdd2.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
//			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
//				// TODO Auto-generated method stub
//				return new Tuple2(arg0._2().split(",")[0],arg0._2().split(",")[1]);
//			}
//		});
//		rdd3.coalesce(1).saveAsTextFile("Data/Fibre2Fashion_output22");
		
	}
}

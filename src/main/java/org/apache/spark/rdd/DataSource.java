package org.apache.spark.rdd;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class DataSource {
	public static void main(String[] args) {
		//Initialization
		SparkConf config = new SparkConf().setMaster("local").setAppName("TopSearchbyUser");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		
		
		
		//File input
		JavaRDD<String> f2fLogRDD = sc.textFile("Data/Fibre2Fashion/F2F.log").distinct();
		JavaRDD<String> stopWordsRDD = sc.textFile("Data/StopWords.txt");
		
		
		
		//Creating PairRDD of required fields
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JavaPairRDD<String, String> f2fPairRDD = f2fLogRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new Tuple2(df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1]),arg0.split(" ")[9]+"-"+df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1])+","+arg0.split(" ")[6]);
				
				
			}
		}).sortByKey();
		
		
		
		//Data Cleaning
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
		//IPPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/IPPairRDD");
		/*
								IPPairRDD sample Data
								(66.249.75.29,Sun May 03 00:00:02 IST 2015-designer swimwear)
								(66.249.75.29,Sun May 03 00:00:06 IST 2015-Campion Marine Inc)
								(66.249.75.21,Sun May 03 00:00:09 IST 2015-textile designer)
		 */
		
		
		
		
		// (IP,term1 term2 term3 ... )
		JavaPairRDD<String, String> termsbyUserPairRDD=IPPairRDD.mapToPair(new PairFunction<Tuple2<String,String>,String, String>() {

			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(), arg0._2().split("-")[1]/*.split(" ")*/);
			}
		});
		//termsbyUserPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/termsByUserPairRDD");
		/*
								termsByUserPairRDD sample Output
								(66.249.75.29,designer swimwear)
								(66.249.75.29,Campion Marine Inc)
								(66.249.75.21,textile designer)
		 */
		
		
		
		
		//(IP,term)
		JavaPairRDD<String, String> singleTermByUserPairRDD = termsbyUserPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
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
		//singleTermByUserPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/singleTermByUserPairRDD");
		/*
		singleTermByUserPairRDD
		(66.249.75.29,designer)
		(66.249.75.29,Campion)
		(66.249.75.21,textile)
		 */
		
		
		
		
		// Removing StopWords
		StopWordsRemoval SWR = new StopWordsRemoval();
		JavaPairRDD<String, String> termsByUser_RemovedStopWords_PairRDD = SWR.removeStopWords(singleTermByUserPairRDD, stopWordsRDD);
		termsByUser_RemovedStopWords_PairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/TermsByUser_RemovedStopWords_PairRDD");
		/*
					termsByUser_RemovedStopWords_PairRDD sample Data
					(124.170.132.39,velvet yarns yarns velvet velvet velvet velvet velvet velvet velvet velvet velvet velvet)
					(66.249.75.21,textile BG AVILTON Campion roll designer Beacon BERMAN YOUSSEF Transaero A.Avrahami KORSAN ARUBA ...)
					(113.11.109.156,Uniclean)
		 */
	}

}

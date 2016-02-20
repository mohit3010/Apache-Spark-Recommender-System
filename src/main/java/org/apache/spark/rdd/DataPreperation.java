package org.apache.spark.rdd;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;import org.apache.spark.sql.functions;
import org.codehaus.janino.Java;

import scala.Tuple2;

public class DataPreperation {
	
	public static void main(String[] args) {
		SparkConf config = new SparkConf().setMaster("local").setAppName("DataPreperation");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<String> custRDD = sc.textFile("Data/DataPreperation/customer.csv");
		JavaRDD<String> transRDD = sc.textFile("Data/DataPreperation/transection.csv");
		
		
		
		
		////Identify distinct rows in Customer.csv
		JavaPairRDD<String, String> CustKP = custRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(x.split(",")[0],x);
			}
		});
		//System.out.println(CustKP.count()+"All rows 25");
		
		//System.out.println(CustKP.keys().distinct()+"distinct rows 25");
		

		JavaPairRDD<String, String> CustKPReduced = CustKP.reduceByKey(new Function2<String, String, String>() {
			
			public String call(String x, String y) throws Exception {
				// TODO Auto-generated method stub``
				return y;
			}
		});
		
		//System.out.println(CustKPReduced.count()+"distinct rows 21");
		//System.out.println(CustKPReduced.collect());


		
		JavaPairRDD<String, String> transKP = transRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(x.split(",")[1], x);
			}
		});
		
		/*JavaPairRDD<String, String> transKpDist = transKP.reduceByKey(new Function2<String, String, String>() {
			public String call(String x, String y) throws Exception {
				// TODO Auto-generated method stub
				return y;
			}
		});*/
		
		
		
		JavaPairRDD<String, Tuple2<String, String>> CustTransKP= CustKPReduced.join(transKP);
		
		
		//System.out.println(CustTransKP.count());

	//	System.out.println(CustKPReduced.take(10))
		
//		System.out.println("Customer Distinct Rows by Key :"+CustKPReduced.count());
//		System.out.println("Total Joined table Rows : "+CustTransKP.count());
//		System.out.println("Distinct Joined Table Rows :"+CustTransKP.distinct().count());
//		System.out.println("Transaction total rows + Distinct Rows:"+transKP.count()+" +" +transKP.distinct().count());
		
		JavaRDD<String> subKeys = CustKPReduced.subtractByKey(CustTransKP).keys();
		
		JavaRDD<String> TotalCustKeys = CustTransKP.distinct().keys();//22797
		JavaRDD<String> TotalCustId = subKeys.union(TotalCustKeys);
	
//		
		//System.out.println(CustTransKP.take(1));
		//JavaRDD<String> transKeys = transKP.distinct().keys();
	
	
		//System.out.println("Expected output: "+transKey.take(10));
		//transKey.coalesce(1).saveAsTextFile("Data/transKey");
		
		/*JavaRDD<Tuple2<String, String>> transId=CustTransKP.values();
		
		JavaRDD<String> transKey = transId.map(new Function<Tuple2<String,String>, String>() {
			public String call(Tuple2<String, String> x) throws Exception {
				// TODO Auto-generated method stub
				return x._1().split(",")[3];
			}
		});
		
		
		CustTransKP.coalesce(1).saveAsTextFile("Data/CustTransKP");
		transId.coalesce(1).saveAsTextFile("Data/transId");
		transKey.coalesce(1).saveAsTextFile("Data/trans_Key");*/
		
		//System.out.println(CustTransKP.take(1));
		//JavaRDD<String> transKey = 
		//System.out.println("Count of tanrsKey:"+transKey.count());
		//System.out.println("First 10: "+transKey.take(10));
		
		JavaRDD<Tuple2<String, String>> CustTransValues = CustTransKP.values();
		JavaRDD<String> CustTrans_transIdRdd = CustTransValues.map(new Function<Tuple2<String,String>, String>() {
			public String call(Tuple2<String, String> x) throws Exception {
				// TODO Auto-generated method stub
				return x._2().split(",")[0];
			}
		});
		
		JavaRDD<String> transIdRDD = transRDD.map(new Function<String, String>() {
			public String call(String x) throws Exception {
				// TODO Auto-generated method stub
				return x.split(",")[0];
			}
		});
		
		//System.out.println(CustTrans_transIdRdd.count());//
		JavaRDD<String> remainingrdd1 = transIdRDD.subtract(CustTrans_transIdRdd);
		//System.out.println(remainingrdd1.count());
		double d = (transIdRDD.count()-CustTrans_transIdRdd.count());
		List<String> list1 = transIdRDD.take((int) d);
		
		JavaRDD<String> Remaining_TransId = sc.parallelize(list1);
		
		JavaRDD<String> total_transIdRdd=CustTrans_transIdRdd.union(Remaining_TransId);
		//CustTrans_transIdRdd.saveAsTextFile("Data/total_transIdRdd");
		
		
		//============================7/1======================================
		
		
		JavaPairRDD<String, Long> TotalCustIdIndexed = TotalCustId.zipWithIndex();
		JavaPairRDD<String, Long> TotalTransIdIndexed= total_transIdRdd.zipWithIndex();
//		TotalCustIdIndexed.coalesce(1).saveAsTextFile("Data/TotalCustIdIndexed");
//		TotalTransIdIndexed.coalesce(1).saveAsTextFile("Data/TotalTransIdIndexed");
		
//		//JavaPairRDD<String, Tuple2<String, Long>> custTransJoined2clmn =TotalCustId.zipWithIndex();
//		//custTransJoined2clmn.coalesce(1).saveAsTextFile("Data/custTransJoined2clmn");
//		JavaPairRDD<Tuple2<String, Tuple2<String, Long>>, Long> custTRansJoined3Clmn=custTransJoined2clmn.zipWithIndex();
//		System.out.println(custTRansJoined3Clmn.take(10));
//		
		JavaPairRDD<Long, String> rdd2 = TotalCustIdIndexed.mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {
			public Tuple2<Long, String> call(Tuple2<String, Long> x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, String>(x._2(), x._1());
			}
		});
		
		//System.out.println(rdd2.take(10));
		//
		JavaPairRDD<Long, String> rdd3 = TotalTransIdIndexed.mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {
			public Tuple2<Long, String> call(Tuple2<String, Long> x) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, String>(x._2(), x._1());
			}
		});
		
		JavaPairRDD<Long, Tuple2<String, String>> rdd4=rdd2.join(rdd3);
		
		rdd4.saveAsTextFile("Data/rdd5");
		
	}

}

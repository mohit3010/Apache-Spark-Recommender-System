package org.apache.spark.rdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import akka.util.Collections;

import scala.Tuple2;

public class CustTransRDD {
	public static void main(String[] args) {
		SparkConf config = new SparkConf().setMaster("local").setAppName("DataPreperation");
		JavaSparkContext sc = new JavaSparkContext(config);
		
		JavaRDD<String> custRDD = sc.textFile("Data/DataPreperation/customer.csv");
		JavaRDD<String> transRDD = sc.textFile("Data/DataPreperation/transection.csv");
		JavaRDD<String> clickStreamRDD = sc.textFile("Data/DataPreperation/ClickStream.csv");
		JavaRDD<String> rdd1 = clickStreamRDD;
		
		
		
		
		JavaRDD<String> mallCloudIdRDD = custRDD.map(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.split(",")[0];
			}
		});
		
		
		
		
		OTP otp = new OTP();
		JavaPairRDD<Long, String> psidCustRDD = custRDD.mapToPair(new PairFunction<String, Long, String >() {
			@Override
			public Tuple2<Long, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, String>((long) ((otp.generateOTP(6)) + 1000000), arg0 );
			}
		});
		
		//psidCustRDD.coalesce(1).saveAsTextFile("Data/custTransRDD/psidCustRDD");
		
		
		
		
		long k = custRDD.count()/5;
		for( long i =0 ; i<=k ; i++ )
		{
			rdd1.union(clickStreamRDD);
		}
		
		System.out.println(rdd1.count());
		
		
		
//		List l = new ArrayList();
//		long k = custRDD.count();
//		for(long j = 0 ; j<= k; j=j+1 )
//		{
//			long i = (long) (Math.random() * 1000000);
//		
//			l.add(i);
//		}
//		
//		JavaRDD<Long> listRDD = sc.parallelize(l);
//		JavaPairRDD<Long, String> randCustRDD = listRDD.zip(custRDD);
//		
//		randCustRDD.coalesce(1).saveAsTextFile("Data/custTransRDD/randCustRDD");
		
//		Set<Long> randomNumSet = new HashSet<Long>();
//		OTP otp = new OTP();
//		
//		while(randomNumSet.size() != custRDD.count()){
//			long random = otp.generateOTP(6);
//			randomNumSet.add(random);
//		}
//		List<Long> randomNumList = new ArrayList<Long>(randomNumSet);
//	
//		JavaRDD<Long> randomNumListRDD = sc.parallelize(randomNumList);
		
		//JavaPairRDD<Long, String> randCustRDD = randomNumListRDD.zip(custRDD);
		//System.out.println(transMallRDD.take(10));
		//System.out.println(custRDD2.take(10));
		//System.out.println(randCustRDD.take(10));
	}
}

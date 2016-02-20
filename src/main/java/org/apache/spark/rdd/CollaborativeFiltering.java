package org.apache.spark.rdd;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;
import scala.Tuple3;
import scala.tools.util.Javap;

class ParseRating implements Function<String, Rating> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Pattern COMMA = Pattern.compile(",");

    @Override
    public Rating call(String line) {
      String[] tok = COMMA.split(line);
      int x = Integer.parseInt(tok[0]);
      int y = Integer.parseInt(tok[1]);
      double rating = Double.parseDouble(tok[2]);
      return new Rating(x, y, rating);
    }
  }

  class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public String call(Tuple2<Object, double[]> element) {
      return element._1() + "," + Arrays.toString(element._2());
    }
  }

public class CollaborativeFiltering {
	public static void main(String[] args) {
		
		// Initialization
		SparkConf config = new SparkConf().setMaster("local").setAppName("TopSearchbyUser");
		JavaSparkContext sc = new JavaSparkContext(config);
		long i = System.currentTimeMillis();
		
		
		
		// Creating RDD of LogFile
		JavaRDD<String> f2fLogRDD = sc.textFile("Data/Fibre2Fashion/F2F.log").distinct();
		
		
		
		
		//Creating PairRDD of required fields
		final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		JavaPairRDD<String, String> f2fPairRDD = f2fLogRDD.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new Tuple2(df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1]),arg0.split(" ")[9]+"-"+df.parse(arg0.split(" ")[0]+" "+arg0.split(" ")[1])+","+arg0.split(" ")[6]);
				
				
			}
		}).sortByKey();
		
		
		
		
		// Data Cleaning
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
		
		
		
		
		// (IP,term1 term2 term3 ... )
		JavaPairRDD<String, String> termsbyUser=IPPairRDD.mapToPair(new PairFunction<Tuple2<String,String>,String, String>() {

			public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0._1(), arg0._2().split("-")[1]/*.split(" ")*/);
			}
		});
		//termsbyUser.coalesce(1).saveAsTextFile("Data/F2FOutput/outputByUser");
		/*
							 SAMPLE OUTPUT
					(66.249.75.29,designer swimwear)
					(66.249.75.29,Campion Marine Inc)
					(66.249.75.21,textile designer)
		 */
		
			
		//----------------------------------------------------------------------------------------------------------------
		
		
		//(IP,term)
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
		//singleTermByUserPairRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/singleTermByUSerPairRDD");
		/*
		 					SAMPLE OUTPUT
							(66.249.75.29,designer)
							(66.249.75.29,Campion)
							(66.249.75.21,textile)		
		 */
		
			
		
		
		JavaRDD<String> distIPRDD = singleTermByUserPairRDD.map(new Function<Tuple2<String,String>, String>() {
		@Override
		public String call(Tuple2<String, String> arg0) throws Exception {
			// TODO Auto-generated method stub
			return arg0._1();
		}
		}).distinct();
//		distIPRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/distIPRDD");
		/*
						      distIPRDD 
							124.170.132.39
							66.249.75.21
							113.11.109.156
		 */
		
		
		
		
		JavaRDD<String> distTermsRDD = singleTermByUserPairRDD.map(new Function<Tuple2<String,String>, String>() {
			@Override
			public String call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2();
			}
		}).distinct();
		//distTermsRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/distTermRDD");
		/*
		 				distTermRDD
						embroidery
						URANUS
						CCI
		 */
		
		
		
		
		OTP otp = new OTP();
		JavaPairRDD<String, Integer> ip_UserIdRDD = distIPRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(arg0,(int) otp.generateOTP(4));
			}
		});
		//ip_UserIdRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/ip_UserIdRDD");
		/*
										ip_UserIdRDD
										(124.170.132.39,1473)
										(66.249.75.21,2653)
										(113.11.109.156,4444)
		 */
		
		JavaPairRDD<String, Integer> term_IDRDD = distTermsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(arg0, (int) otp.generateOTP(6));
			}
		});
		//term_IDRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/term_IDRDD");
		/*
						(embroidery,818155)
						(URANUS,522131)
						(CCI,722046)
		 */
		
		
		JavaPairRDD<String, Tuple2<Integer, String>> joinedRDD = ip_UserIdRDD.join(singleTermByUserPairRDD);
		
//		joinedRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/JoinedRDD");
		
		
		JavaPairRDD<String, String> joinedRDDv2 = joinedRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,String>>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<Integer, String>> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(arg0._2()._2(), arg0._1()+","+arg0._2()._1());
			}
		});
//		joinedRDDv2.coalesce(1).saveAsTextFile("Data/F2FOutput/joinedRDDv2");
		JavaPairRDD<String, Tuple2<Integer, String>> wholeRDD = term_IDRDD.join(joinedRDDv2);
//		wholeRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/finalRDD");
		
		JavaRDD<String> dataRDD = wholeRDD.map(new Function<Tuple2<String,Tuple2<Integer,String>>, String>() {
			@Override
			public String call(Tuple2<String, Tuple2<Integer, String>> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2()._2().split(",")[1]+","+arg0._2()._1();
			}
		});
//		dataRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/dataRDD");
		
		JavaPairRDD<String, Integer> dataRDD2 = dataRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(arg0.split(",")[0]+" "+arg0.split(",")[1], 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
//		dataRDD2.coalesce(1).saveAsTextFile("Data/F2FOutput/dataRDD2");
		
		JavaRDD<String> finalDataRDD = dataRDD2.map(new Function<Tuple2<String,Integer>, String>() {
			@Override
			public String call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._1().split(" ")[0]+","+arg0._1().split(" ")[1]+","+arg0._2();
			}
		});
//		finalDataRDD.coalesce(1).saveAsTextFile("Data/F2FOutput/finalDataRDD");
		
		//***************************** A L S ***************************************
		
		
		 int rank = 10;
		    int iterations = 3;
		    String outputDir = "Data/RSOutput/2";
		    int blocks = -1;
		    

		    
		    

		    JavaRDD<Rating> ratings = finalDataRDD.map(new ParseRating());

		    MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);

		    model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
		        outputDir + "/userFeatures");
		    model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(
		        outputDir + "/productFeatures");
		    System.out.println("Final user/product features written to " + outputDir);

		    sc.stop();
		
		
		
		
		
		
		
		
		
		long j = System.currentTimeMillis();
		System.out.println("Time taken: "+(j-i)/1000+" s");
	}

}
 

////***************************** Recommender System ************************************************************************
//
//
//
//JavaRDD<Rating> ratings = finalDataRDD.map(
//  new Function<String, Rating>() {
//    public Rating call(String s) {
//      String[] sarray = s.split(",");
//      return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
//        Double.parseDouble(sarray[2]));
//    }
//  }
//);
//
//// Build the recommendation model using ALS
//int rank = 10;
//int numIterations = 10;
//MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
//
//// Evaluate the model on rating data
//JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
//  new Function<Rating, Tuple2<Object, Object>>() {
//    public Tuple2<Object, Object> call(Rating r) {
//      return new Tuple2<Object, Object>(r.user(), r.product());
//    }
//  }
//);
//JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
//  model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
//    new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
//      public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
//        return new Tuple2<Tuple2<Integer, Integer>, Double>(
//          new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
//      }
//    }
//  ));
//JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
//  JavaPairRDD.fromJavaRDD(ratings.map(
//    new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
//      public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
//        return new Tuple2<Tuple2<Integer, Integer>, Double>(
//          new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
//      }
//    }
//  )).join(predictions).values();
//double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
//  new Function<Tuple2<Double, Double>, Object>() {
//    public Object call(Tuple2<Double, Double> pair) {
//      Double err = pair._1() - pair._2();
//      return err * err;
//    }
//  }
//).rdd()).mean();
//System.out.println("Mean Squared Error = " + MSE);
//
//// Save and load model
//model.save(sc.sc(), "Data/RSOutput/1");
//MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(),
//  "Data/RSOutput/1");
//

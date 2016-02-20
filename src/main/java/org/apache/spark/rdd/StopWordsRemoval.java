/**
 * Created by Niyat Patel 
 * Date : 20/02/2016
 * 
 * Returns JavaPairRDD with no StopWords
 * 
 * @param singleTermByUserPairRDD JavaPairRDD with single term as a value
 * @param stopWordsRDD JavaRDD containing StopWords
 * 
 */
package org.apache.spark.rdd;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;

import scala.Tuple2;

public class StopWordsRemoval implements Serializable{
	/**
	 * Returns JavaPairRDD with no StopWords
	 * 
	 * @param singleTermByUserPairRDD : JavaPairRDD with single term as a value
	 * @param stopWordsRDD : JavaRDD containing StopWords
	 * 
	 */
	public JavaPairRDD<String, String> removeStopWords(JavaPairRDD<String, String> singleTermByUserPairRDD,JavaRDD<String> stopWordsRDD)
	{
		
		List<String> stopWordsList = stopWordsRDD.toArray(); 
		JavaPairRDD<String, String>  TermByUser_RemovedStopwords_PairRDD = singleTermByUserPairRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> line) throws Exception {
				// TODO Auto-generated method stub
				for (String s : stopWordsList) {
					if(line._2().equalsIgnoreCase(s))
					{
						return new Tuple2<String, String>(line._1(),"null");
					}
				}
				return line;
			}
			}).filter(new Function<Tuple2<String,String>, Boolean>() {
				
				@Override
				public Boolean call(Tuple2<String, String> arg0) throws Exception {
					// TODO Auto-generated method stub
					return !arg0._2().equals("null");
				}
			}).reduceByKey(new Function2<String, String, String>() {
				
				@Override
				public String call(String arg0, String arg1) throws Exception {
					// TODO Auto-generated method stub
					return arg0+" "+arg1;
				}
			});
		
			return TermByUser_RemovedStopwords_PairRDD;
	}
}

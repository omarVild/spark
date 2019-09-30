package com.spark.nycityairbnb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperations {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		SQLContext sqlC = new SQLContext(sc);
		String nyCityAirbnb = "dataset/AB_NYC_2019.csv";
		
		Dataset<Row> nyAirbnb = sqlC.read().format("csv").option("sep", ";").option("inferSchema", "true").
				option("header", "true").load(nyCityAirbnb);
		
		/*
		 * Filtering by price
		 */
		Dataset<Row> lowPrices = nyAirbnb.filter("'price'>='100'");
		
		lowPrices.foreach(new ForeachFunction() {
			@Override
			public void call(Object t) throws Exception {
				System.out.println(t.toString());
			}
		});
		System.out.println("****END");
	}
}

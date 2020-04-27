package com.sinward.rec_sys.OfflineCalculater;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

import org.apache.spark.SparkConf;

/*spark SQL 模块*/
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkSession spark = SparkSession
    			  .builder()
    			  .master("local")
    			  .appName("OfflineCalculater")
    			  .getOrCreate();
    	
    	Dataset<Row> jdbcDF = spark.read()
    			  .format("jdbc")
    			  .option("url", "jdbc:postgresql://localhost/test")
    			  .option("dbtable", "(SELECT url,content,docid FROM page_content) AS temptable")
    			  .option("user", "postgres")
    			  .option("password", "Ye25554160")
    			  .load();
    	jdbcDF.show();
    }
}

package com.akg92;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.akg92.utils.MLHelper;
public class Main {

	public static final String APP_NAME = "personal";

	public static void main(String[] args) throws Exception {
		String inputFile = args[0] ;
		String outputFile = args.length > 1 ? args[1] : null;
		MLHelper helper = new MLHelper(inputFile, outputFile);
		helper.train();
	}
}

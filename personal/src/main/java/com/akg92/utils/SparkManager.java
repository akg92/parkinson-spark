package com.akg92.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

// use singleton method
class SparkManager{
    private JavaSparkContext sc = null;
    private SparkSession session = null; 
    private static SparkManager sparkManager = null;

    // return spark context
    public JavaSparkContext getContext(){
        if( sc == null){
            sc = new JavaSparkContext(new SparkConf());
        }
        return sc;
    }
    // get spark session
    public SparkSession getSession(){
        if(session == null){
            session = SparkSession.builder().config(getContext().getConf()).getOrCreate();
        }
        return session;
    }
    // get instance.
    public static SparkManager getInstance(){
        
        if(sparkManager == null){
            sparkManager = new SparkManager();
        }
        return sparkManager;
    }
    
}
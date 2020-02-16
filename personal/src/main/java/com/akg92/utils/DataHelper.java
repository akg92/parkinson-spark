package com.akg92.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


// import org.apache.spark.mllib;
// import org.apache.spark
class DataHelper{
    private String fileName;
    // constructor
    public DataHelper(String fileName){
        this.fileName = fileName;
    }

    // get file content from csv.
    private Dataset<Row> readCSV(String fileName){
       SparkSession session = SparkManager.getInstance().getSession();
       return session.read().format("csv").option("header", true).option("inferSchema", true).load(fileName);
    }
    // this method is to keep the abstraction for future.
    // currently it very simple to avoid over engineering.
    public Dataset<Row> read(){
        return readCSV(this.fileName);
    }
    // copy array utitlity
    private String [] arrayCopy(String[] input, String exclude){
        String [] result = new String[input.length-1];
        
        for(int i = 0, index = 0; i < input.length-1; i++){
            if(!input[i].equals(exclude)){
                result[index++] = input[i];
            }
        }
        return result;
    }
    
    private int getColumnIndex(String[] columns, String target){
        for(int i = 0; i < columns.length; i++){
            if(columns[i].equals(target)){
                return i;
            }
        }
        return -1;

    }
    
    public void save(String fileName, JavaRDD<Tuple2> rdd){
        // SparkSession session = SparkManager.getInstance().getSession();
        rdd.saveAsTextFile(fileName);

    }
    // get java rdd
    // public JavaRDD<LabeledPoint> readAsLabeledPoints(String target, String []deleteColumns){ 
    //     Dataset<Row> dataset = read();    
    //     // String[] dropColumns = {};
    //     dataset = dataset.drop(deleteColumns);
    //     String [] allColumns = dataset.columns();
        
    //     int targetIndex = getColumnIndex(allColumns, target);
    //     return dataset.toJavaRDD().map( (row)->{
            
            
    //         double[] values = new double[row.length()-1];
    //         for(int i = 0, index = 0; i < row.length(); i++){
    //             if(i != targetIndex){
    //                 values[index++] = row.getDouble(i);
    //             }
    //         }
            
    //         return new LabeledPoint(row.getDouble(targetIndex), Vectors.dense(values));
    //     });
        
        // String []features = arrayCopy(allColumns, target);
        // // String target = allColumns[allColumns.length - 1];

        // VectorAssembler assembler =  new VectorAssembler();
        // assembler.setInputCols(features).setOutputCol(target);
        // assembler.transform(dataset).toJavaRDD().map( row -> {

        // });
        // dataset.toJavaRDD().map( (row)-> {
        //     return new LabeledPoint(label, features)
        // })
        // return assembler.transform(dataset);
        // return assembler;
    // }

}
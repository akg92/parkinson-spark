package com.akg92.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;
import scala.collection.Seq;

public class MLHelper {

    private MLConfig mlConfig;

    // constructors
    // default test train split
    public MLHelper(String inputFileName, String outputFileName) {
        mlConfig = new MLConfig();
        mlConfig.setInputFileName(inputFileName);
        mlConfig.setOutFileName(outputFileName);
    }

    public MLHelper(String inputFileName, String outputFileName, float trainSplit) {
        mlConfig = new MLConfig();
        mlConfig.setInputFileName(inputFileName);
        mlConfig.setOutFileName(outputFileName);
        mlConfig.setTrainSplit(trainSplit);
    }

    // for future.
    public MLHelper(MLConfig mlConfig) {
        this.mlConfig = mlConfig;
    }

    private String [] arrayCopy(String[] input, String exclude){

        String [] result = new String[input.length-1];
        
        for(int i = 0, index = 0; i < input.length; i++){
            if(!input[i].equals(exclude)){
                result[index++] = input[i];
            }
        }
        for(String str : result){
            System.out.println(str +"%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        }
        return result;
    }
    // train
    public void train() {
        String inFileName = mlConfig.getInputFileName();
        DataHelper helper = new DataHelper(inFileName);
        Dataset<Row> data = helper.read();
        data = data.drop(mlConfig.getDeleteColumn());
        // Dataset<Row>[] splits = data.randomSplit(new
        // double[]{mlConfig.getTrainSplit(), 1- mlConfig.getTrainSplit() });
        // Dataset<Row> testData = splits[1];
        // Dataset<Row> trainData = splits[0];

        // RandomForestClassifier
        // JavaRDD rdd = testData.toJavaRDD().map;
        // RandomForestModel model = RandomForest.trainRegressor( trainData.toJavaRDD(),
        // mlConfig.getCategoricalFeatureInfo(),
        // mlConfig.getNumberOfTrees(),
        // mlConfig.getFeatureSubsetStrategy(),
        // mlConfig.getImpurity(),
        // mlConfig.getMaxDepth(),
        // mlConfig.getMaxBin(),
        // mlConfig.getSeed());
        // );

        StringIndexerModel labelIndexer = new StringIndexer().setInputCol(mlConfig.getTargetColumn()).setOutputCol("indexedLabel")
                .fit(data);
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 4 distinct values are treated as
        // continuous.
        // VectorIndexerModel featureIndexer = new VectorIndexer().setOutputCol("indexedFeatures").setMaxCategories(4).fit(data);
        VectorAssembler featureIndexer = new VectorAssembler().setInputCols(arrayCopy(data.columns(), mlConfig.getTargetColumn()))
            .setOutputCol("indexedFeatures");
        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = data.randomSplit(new double[] { 0.7, 0.3 });
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier().setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures").setSeed(mlConfig.getSeed());

        // Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] { labelIndexer, featureIndexer, rf, labelConverter });

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);
        // predictions.show(5);
        JavaRDD<Tuple2> rdd = calculateMetric(predictions);
        helper.save(mlConfig.getOutFileName(), rdd);
    }

    private JavaRDD<Tuple2> calculateMetric(Dataset<Row> result){
        // MLConfig mlConfig = new MLConfig();
        Dataset<Row> predictionAndLabels =  result.select(result.col(mlConfig.getTargetColumn()), result.col("predictedLabel")).cache();
        // JavaRDD<Tuple> tpl= predictionAndLabels.map( (row)-> {return new Tuple<Double, Double>(row[0],row[1]);});
       
        for (String c: predictionAndLabels.columns()) {
            predictionAndLabels = predictionAndLabels.withColumn(c, predictionAndLabels.col(c).cast("Double"));
        }
       
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        double precision = metrics.precision(0);
        double recall = metrics.recall(0);

        System.out.println( "Precision = "+precision+" Recall = "+recall);
        // // Recall by threshold
        //  JavaRDD<?> recall = metrics.reca().toJavaRDD();
        //  System.out.println("Recall by threshold: " + recall.collect());
        double auroc = new BinaryClassificationMetrics(predictionAndLabels).areaUnderROC();
         System.out.println("Area under ROC = " + auroc);

         List<Tuple2> list  = new ArrayList<>();
         list.add( new Tuple2<>("precision", precision));
         list.add( new Tuple2<>("recall", recall));
         list.add( new Tuple2<>("auroc", precision));
         
        return SparkManager.getInstance().getContext().parallelize(list);
    
        //  ({
        //      ("precicision",precision),
        //      ("recall",recall),
        //      ("auroc",auroc)
        //     }).toDf();

    }

}
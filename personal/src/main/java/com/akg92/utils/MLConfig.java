package com.akg92.utils;

import java.util.HashMap;
import java.util.Map;

// helper class to manger training config
public class MLConfig{

    // All the configuration is stored in Map for the unification.
    private Map<String, String> configMap;
    private static float defaultTrainSpit = 0.8f;

    public MLConfig(){
        configMap = new HashMap<String, String>();
    }

    // Below are set of common helper function to acheive the common operation
    
    // input file
    public String getInputFileName(){
        return configMap.get("in_file");
    }
    public void setInputFileName(String file){
        configMap.put("in_file", file);
    }
    
    // outpu file
    public String getOutFileName(){
        return configMap.containsKey("out_file") && configMap.get("out_file") != null ?  configMap.get("out_file") : "ml_test" ;
    }
    public void setOutFileName(String file){
        configMap.put("out_file", file);
    }

    // train split 
    public Float getTrainSplit(){
        return configMap.containsKey("train_split") ? Float.parseFloat(configMap.get("train_split")) :
            defaultTrainSpit;
    }
    public void setTrainSplit(Float splitPercentage){
        configMap.put("train_split", splitPercentage.toString());
    }

    // Random forest specific configs. currently harcoding.
    // Keep the interface for future work.
    public Map<Integer, Integer> getCategoricalFeatureInfo(){
        return new HashMap<Integer,Integer>();
    }
    public int getNumberOfTrees(){
        return 3;
    }
    public int getMaxDepth(){
        return 4;
    }
    public String getFeatureSubsetStrategy(){
        return "auto";
    }
    public String getImpurity(){
        return "variance";
    }
    public int getMaxBin(){
        return 32;
    }
    public int getSeed(){
        return 1;
    }
    public String[] getDeleteColumn(){
        String deleteColumList[] = {"name"};
        return  deleteColumList;
    }
    public String getTargetColumn(){
        return "status";
    }
}
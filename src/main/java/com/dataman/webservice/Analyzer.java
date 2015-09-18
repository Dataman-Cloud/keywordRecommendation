package com.dataman.webservice;


import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.mllib.clustering.LocalLDAModel;

import com.dataman.nlp.util.StanfordSegment;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;

public class Analyzer {
    public static CRFClassifier<CoreLabel> analyzer;
    public static LocalLDAModel model;
    public static SparkContext sc;
    public static SQLContext sqlContext;
    
    static {
        Analyzer.analyzer = StanfordSegment.wordSegment("http://10.3.12.2:8666/analyzer");
        Analyzer.sc = new SparkContext();
        Analyzer.sqlContext = new SQLContext(sc);
        Analyzer.model = LocalLDAModel.load(Analyzer.sc, "hdfs://10.3.12.9:9000/model/one");
    }
}

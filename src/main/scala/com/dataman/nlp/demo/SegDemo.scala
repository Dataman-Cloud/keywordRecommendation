package com.dataman.nlp.demo

import java.io.PrintStream
import java.util.Properties

import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.parser.lexparser.LexicalizedParser

/**
  *  Created by fchen on 14-11-30.
  */
object SegDemo {
  def main(args: Array[String]) {
    //      System.out.println("basedir=====" + basedir)
    //val basedir = System.getProperty("user.dir")
    val basedir = "."
    System.setOut(new PrintStream(System.out, true, "utf-8"))
    val props: Properties = new Properties
    props.setProperty("sighanCorporaDict", "http://192.168.100.4/deploy/nlp/data")
    props.setProperty("serDictionary", "http://192.168.100.4/deploy/nlp/data/dict-chris6.ser.gz")
    if (args.length > 0) {
      props.setProperty("testFile", args(0))
    }
    props.setProperty("inputEncoding", "UTF-8")
    props.setProperty("sighanPostProcessing", "true")
    println("=====property=====")
    props.list(System.out)
    println("==================")

    val segmenter: CRFClassifier[CoreLabel] = new CRFClassifier[CoreLabel](props)


    //val is = this.getClass.getResourceAsStream("/data/ctb.gz")
    //val is = this.getClass.getResourceAsStream("http://192.168.100.4/deploy/nlp/data/ctb.gz")
    val is = new java.net.URL("http://192.168.100.4/deploy/nlp/data/ctb.gz").openStream
    val inputStream = new java.util.zip.GZIPInputStream(new java.io.BufferedInputStream(is))
    segmenter.loadClassifierNoExceptions(inputStream, props)
    //segmenter.loadClassifierNoExceptions("src/main/resources/data/ctb.gz", props)
    //val url = "http://192.168.100.4/deploy/nlp/data/ctb.gz"
    //val is = new java.net.URL(url).openStream
    //segmenter.loadClassifierNoExceptions(is, props)
    for (filename <- args) {
      segmenter.classifyAndWriteAnswers(filename)
    }
    val sample: String = "《戴着红色项链的T夫人》（Mrs. T with a Red Nacklace, 1930年）■埃米尔·诺尔德是德国最受人喜爱的表现主义艺术家，但多年以来，这位多产的画家也因其曾支持纳粹的经历而饱受历史的争议。"
    val segmented: java.util.List[String] = segmenter.segmentString(sample)
    System.out.println(segmented)

  }
  //    private final val basedir: String = System.getProperty("SegDemo", "data")
}

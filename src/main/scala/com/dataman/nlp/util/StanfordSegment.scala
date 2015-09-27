package com.dataman.nlp.util

/**
 * Created by ckb on 2015/8/20.
 */

import java.util.Properties
import java.util.zip.GZIPInputStream
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import java.io.{BufferedInputStream, PrintStream, Serializable,InputStream,FileInputStream}
import java.net.{MalformedURLException, URL,URI}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.jsoup.Jsoup

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils

object StanfordSegment {

    def wordSegment(baseURL:String):CRFClassifier[CoreLabel]={
      /*
      val props = new Properties
      props.setProperty("sighanCorporaDict", baseURL)
      //props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
      props.setProperty("inputEncoding", "UTF-8")
      props.setProperty("sighanPostProcessing", "true")
      val url = s"$baseURL/ctb.gz"

      val segmenter= new CRFClassifier[CoreLabel](props)
      val conf=new Configuration()
      val path=URI.create(url)
      val inputFs=FileSystem.get(path,conf)
      val is =inputFs.open(new Path(url))
      //val is = new java.net.URL(url).openStream()
      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
      segmenter.loadClassifierNoExceptions(inputStream, props)
      inputStream.close
      is.close
      segmenter
      */
      val props = new Properties
      props.setProperty("sighanCorporaDict", baseURL)
      props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
      props.setProperty("inputEncoding", "UTF-8")
      props.setProperty("sighanPostProcessing", "true")
      val url = s"$baseURL/ctb.gz"
      val segmenter= new CRFClassifier[CoreLabel](props)
      val is = new java.net.URL(url).openStream()
      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
      segmenter.loadClassifierNoExceptions(inputStream, props)
      inputStream.close
      is.close
      segmenter
    }
    def articleSegment(baseURL:String):CRFClassifier[CoreLabel]={
      val props = new Properties
      props.setProperty("sighanCorporaDict", baseURL)
      props.setProperty("serDictionary", s"$baseURL/dict-chris6.ser.gz")
      props.setProperty("inputEncoding", "UTF-8")
      props.setProperty("sighanPostProcessing", "true")
      val url = s"$baseURL/ctb.gz"
      val segmenter= new CRFClassifier[CoreLabel](props)
     // val is = new java.net.URL(url).openStream()
      val is =new FileInputStream(url)
      val inputStream = new GZIPInputStream(new BufferedInputStream(is))
      segmenter.loadClassifierNoExceptions(inputStream, props)
      inputStream.close
      is.close
      segmenter


    }

   def main(args: Array[String]) {

     //val str="诺尔德是德国最受人喜爱的表现主义艺术家，但多年以来，这位多产的画家也因其曾支持纳粹的经历而饱受历史的争议"
     val str="<p><img class=\\\"lazy_image\\\" href=\\\"http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg\\\" \\\" onclick=\\\"javascript:iosuri('slate:\\/\\/gallery\\/http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_031382mr.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg')\\\" ><p>疗伤系的日剧大体总是这样的情节，主人公从大都会里黯然回到故乡，除了感受到浓郁的人情之外，还总能在一两样熟悉的食物里找回童年的、阿婆时代的味道。而蒸菜，其实也是一种疗伤系的食物，它的味道吃起来总是那么熟悉。<p>\\r\\n<p>\\r\\n“蒸”其实是中国美食里非常重要的烹饪手法之一，比起“煎”、“炸”、“煮”，蒸菜可不单是蒸蛋那么贫乏。蒸菜在中国其实已经5000年以上的历史。最早出名的是“清蒸”，以“汤清、色明、味鲜、质嫩、气香、纯正”著称。之后，蒸菜经过历史的沿革，经由老百姓的民间智慧，逐渐发展成一门独特的烹饪学问。如传统的蒸菜里面，便有清蒸、干蒸、糟蒸、粉蒸、包蒸和上浆蒸等等，如今则还有如同火锅般边蒸边吃的新派蒸菜。<p>\\r\\n<p>\\r\\n<p><img class=\\\"lazy_image\\\" href=\\\"http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_031382mr.jpg\\\" \\\" onclick=\\\"javascript:iosuri('slate:\\/\\/gallery\\/http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_031382mr.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg')\\\" ><p>所谓边蒸边吃，就是你所吃到的菜，不是从厨房里蒸好再端到桌面来的，而是客人点单以后，将各色菜式，端到餐桌上，边蒸边吃。要几分熟，是熟一点还是脆一点，全凭客人定夺，你甚至也可以要求自己上手蒸菜。只不过受过训练的服务生在蒸菜的时候，火候会拿捏得更加好一些。如此蒸出来的菜，堪称最新鲜，无论是蔬菜还是海鲜，都有一股鲜甜的滋味，即使是不加任何调味料也是有味的。而且，据说蒸菜之所以健康，是因为温度最高只达到100度，所以可以比较全面地留住营养。而现蒸现吃，可是更加新鲜的吃法。<p>\\r\\n<p>\\r\\n雅科餐厅有道蒸菜居然叫“海宝”，其实是海中三宝的意思，包括草虾、目鱼仔和鲜带子，然后搭配上生菜、卷心菜、西洋芹之类的蔬菜。吃的时候，先蒸蔬菜，趁热吃，然后再蒸海鲜。只需要一两分钟，海鲜便可入口，如此鲜甜的口感，似乎很难在其他做法的菜式里吃到。<p>\\r\\n<p>\\r\\n<h3>地址：上海市南京西路248号大光明电影院三楼屋顶花园<\\/h3><p>\\r\\n<p>\\r\\n<p><img class=\\\"lazy_image\\\" href=\\\"http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg\\\" \\\" onclick=\\\"javascript:iosuri('slate:\\/\\/gallery\\/http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_031382mr.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/59_0yrnpzvj.jpg')\\\" ><p>"
     val text = Jsoup.parse(str).text()
     //println(text)

     val baseURL="/analyzer"
     val result=wordSegment(baseURL).segmentString(str).toArray().mkString(" ")
     println(result)

  }
}

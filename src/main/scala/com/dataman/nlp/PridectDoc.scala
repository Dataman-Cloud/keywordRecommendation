package com.dataman.nlp



import com.dataman.nlp.util.{RelMysql,Mysql}
import org.apache.spark.sql.{SQLContext,Row}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel,OnlineLDAOptimizer,EMLDAOptimizer,LDA,LocalLDAModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable
import java.text.BreakIterator
import java.sql.{DriverManager,PreparedStatement,Connection}
import org.apache.spark.rdd.{JdbcRDD,RDD}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.types._
import com.dataman.nlp.util.Mysql
import com.dataman.nlp.knn._
import scala.collection.immutable.Vector

import org.jsoup.Jsoup
import com.dataman.nlp.util.StanfordSegment

/**
 * Created by ener on 8/25/15.
 */


object PridectDoc {

    def main(args: Array[String]) {


        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val lda = LocalLDAModel.load(sc, "hdfs://10.3.12.9:9000/test/ModelMatrix/Matrix1")
        /*
        val input = lda.describeTopics(10).map(x => x._2).map(y => y.toVector)
        val index1 = input.zipWithIndex.map(x => {
            val lo = x._2.toLong
            (x._1, lo)
        })
        //println("get topic")
        val rddinput = sc.parallelize(index1, 10)
        val inputDev = input.toVector
        */
       // rdd.map(_.replaceAll("\\]|\\)", "").split("\\[")(1).split(",").map(_.toDouble)).map(_.toVector).collect
        //val vec=(x.replaceAll(s"\\]|\\)","").split("\\[")(1).split(",").map(_.toDouble)).map()
        val artRdd:RDD[String]=sc.textFile("hdfs://10.3.12.9:9000/test/PrWord/Word1/",10)
        val inputRdd=artRdd.map(x=>{x.replaceAll(s"\\]|\\)","").split("\\[")
          }).map(y=>{
            val vec=(y(1).split(",").map(_.toDouble)).toVector
            val index=y(0).replaceAll(s"\\(|\\,","").toLong
            (vec,index)
        })

        //val str =args(0).toString
        //val str="古代阿兹特克人发现了可可树，据说其国王每天要喝30杯，请看下面这张图，图片来自墨西哥，是两位印第安酋长在喝巧克力。<p>\\r\\n<p><img class=\\\"lazy_image\\\" href=\\\"http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_d3m44x12.jpg\\\" \\\" onclick=\\\"javascript:iosuri('slate:\\/\\/gallery\\/http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_d3m44x12.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_q9vkde6n.jpg')\\\" ><p><p>\\r\\n据考证，当时的玛雅人喝的巧克力里面还要添加香草、蜂蜜、甚至辣椒（有点匪夷所思啊），不过有一点很重要，玛雅人喝巧克力是不加奶的，只是混合在水里。把巧克力融化在煮沸的牛奶里这是瑞士人的发明。<p>\\r\\n<p>\\r\\n注意，最早的巧克力真的是用“喝”的，要知道，这可是世界三大“饮料”之一啊！其余两者是咖啡和茶。<p>\\r\\n<p>\\r\\n我们现在越来越多会把固体的巧克力拿在手里咬来吃，“喝”巧克力的方式慢慢淡出主流，但在甜品店还可以吃到。<p>\\r\\n<p>\\r\\n[float=right][img]http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_hhby7a06.jpg[\\/img][\\/float]实际上，如果要把巧克力做成饮品会有非常丰富的做法，个人感觉，比溶掉巧克力倒在模具里的所谓手工巧克力要有趣得多。今后容我慢慢为大家介绍。<p>\\r\\n<p>\\r\\n再多提一个问题，知道热可可和热巧克力有什么差别吗？这个问题相信很多人还是很混淆的，甚至一些甜点店的制作者自己也没搞明白。<p>\\r\\n<p>\\r\\n热可可和热巧克力当然是完全不一样的，热可可是用可可粉融化在牛奶里，而热巧克力则是用巧克力条融化在牛奶里。<p>\\r\\n<p>\\r\\n虽然可可粉和巧克力条都是可可豆的制成品，但是两者最重要差别就是可可油脂的含量了。可可粉一般是脱脂的，也有可能含少量的可可脂10%-12%左右，而优质的巧克力条则应该是富含可可脂，可可脂会高达22%以上。所以，热巧克力视觉上就会有更浓稠的感觉，口感更厚实、风味更悠长。<p>\\r\\n<p>\\r\\n嗯，晚上用新到的法芙娜厄瓜多尔牛奶巧克力做了两杯热巧克力，然后抹了一点新鲜奶油，再撒些许可可粉，和丢帕一人一杯，有点甜腻，但是很满足。;-) 天气转凉，我们会给大家介绍更多的热巧克力做法，甚至和咖啡混合的做法。<p>\\r\\n<p><img class=\\\"lazy_image\\\" href=\\\"http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_q9vkde6n.jpg\\\" \\\" onclick=\\\"javascript:iosuri('slate:\\/\\/gallery\\/http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_d3m44x12.jpg,http:\\/\\/s2.cdn.iweekly.bbwc.cn\\/beta\\/201002\\/08\\/42_q9vkde6n.jpg')\\\" ><p>"
        val str=args(1).toString
        val str_artid=args(0).toString
        val text = Jsoup.parse(str).text()
        val baseURL="http://10.3.12.2:8666/analyzer"
        val result=StanfordSegment.wordSegment(baseURL).segmentString(text).toArray().mkString(" ").replaceAll(s"\\r\\n","").replaceAll("\\pP|\\pS","").replaceAll("[a-zA-Z]","").replaceAll(" +"," ")
       // val list = List("hdfs://10.3.12.9:9000/test/bbc/dataone")
        //val list = List("hdfs://10.3.12.9:9000/test/VectorWord/word13")
        val docVec = ToVector.strToVector(sc, result, 100000, "hdfs://10.3.12.9:9000/test/stopword.dic")
        //docVec.saveAsTextFile("hdfs://10.3.12.9:9000/test/PrWord/Word9")
        val preDeV = lda.topicDistributions(docVec)
        //preDeV.repartition(1).saveAsTextFile("hdfs://10.3.12.9:9000/test/PrWord/Word6")
          .map(x => x._2.toArray)
          .map(y => {
            y.toVector
        }).take(1)(0)
        println("get one doc topic")
        val vec2 = knnJoin.knnJoin(inputRdd,preDeV,5,10,sc)
        val artid=vec2.map(x=>x._2.toString).collect().mkString(",")
        val recommend_article="{"+str_artid+":"+artid+"}"
        /*
        val query=s"delete from recommend_article"
        val query1 =s"INSERT INTO recommend_article (recommend_article) VALUES " + "('" + recommend_article + "')"
        Mysql.insert(query)
        Mysql.insert(query1)
        */

        //vec2.repartition(1).saveAsTextFile("hdfs://10.3.12.9:9000/test/preDoc3")

        //println("success")

    }
    

}

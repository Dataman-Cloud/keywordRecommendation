package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Created by fchen on 15-9-16.
 */
class DMLDA(
             private var k: Int,
             private var maxIterations: Int,
             private var docConcentration: Vector,
             private var topicConcentration: Double,
             private var seed: Long,
             private var checkpointInterval: Int,
             private var ldaOptimizer: LDAOptimizer
             ) extends LDA {

  def this() = this(k = 10, maxIterations = 20, docConcentration = Vectors.dense(-1),
    topicConcentration = -1, seed = Utils.random.nextLong(), checkpointInterval = 10,
    ldaOptimizer = new EMLDAOptimizer)

  override def run(documents: RDD[(Long, Vector)], sc: SparkContext = null ): LDAModel = {
    val state = ldaOptimizer.initialize(documents, this)
    var iter = 0
    val iterationTimes = Array.fill[Double](maxIterations)(0)
    while (iter < maxIterations) {
      val start = System.nanoTime()
      state.next()
      val elapsedSeconds = (System.nanoTime() - start) / 1e9
      iterationTimes(iter) = elapsedSeconds
      iter += 1
      //******************
      if (sc != null && iter % 3 == 0) {
        val topicIndices = state.getLDAModel(iterationTimes).describeTopics(maxTermsPerTopic = 10)
        val topics = topicIndices.map {
          case (terms, termWeights) =>
            terms.zip(termWeights).map {
              case (term, weight) => s"$term:$weight"
            }.mkString("\t")
        }

        topics.foreach(println)
      }
    }
    state.getLDAModel(iterationTimes)
  }
}

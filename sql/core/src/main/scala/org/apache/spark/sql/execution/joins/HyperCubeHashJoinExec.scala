
package org.apache.spark.sql.execution.joins

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

 /**
  * Created by haotan on 17/11/26.
  */
case class HyperCubeHashJoinExec(leftKeys: Seq[Expression],
                                 rightKeys: Seq[Expression],
                                 joinType: JoinType,
                                 buildSide: BuildSide,
                                 condition: Option[Expression],
                                 left: SparkPlan,
                                 right: SparkPlan,
                                 leftRDD: RDD[InternalRow] = null,
                                 rightRDD: RDD[InternalRow] = null)
  extends BinaryExecNode with HashJoin {

   private def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(iter, buildKeys, taskMemoryManager = context.taskMemoryManager())
    buildTime += (System.nanoTime() - start) / 1000000
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener(_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val myLeftRDD = if (leftRDD == null) {
      left.execute()
    } else {
      leftRDD
    }

    val myRightRDD = if (rightRDD == null) {
      right.execute()
    } else {
      rightRDD
    }

    myLeftRDD.zipPartitions(myRightRDD) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      join(streamIter, hashed, numOutputRows)
    }
  }
}

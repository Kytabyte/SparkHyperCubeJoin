/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics



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
                                 leftRDD: Option[RDD[InternalRow]],
                                 rightRDD: Option[RDD[InternalRow]])
  extends BinaryExecNode with HashJoin {

   override lazy val metrics = Map(
     "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
     "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
     "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))


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
    val myLeftRDD = if (leftRDD == None) {
      left.execute()
    } else {
      leftRDD.get
    }

    val myRightRDD = if (rightRDD == None) {
      right.execute()
    } else {
      rightRDD.get
    }

    myLeftRDD.zipPartitions(myRightRDD) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      join(streamIter, hashed, numOutputRows)
    }
  }
}

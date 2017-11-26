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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, MultaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class HyperCubeJoinExec(joinKeys: Seq[Seq[Expression]],
                            joinMatrix: Seq[Seq[Expression]],
                             conditions: Seq[Seq[Option[Expression]]],
                             nodes: Seq[SparkPlan])
  extends MultaryExecNode {

  override def output: Seq[Attribute] = nodes.reduceLeft(_.output ++ _.output)

//  val buildSide = BuildLeft
//  val joinType = Inner
//  val left = nodes(0)
//  val right = nodes(1)
//  val condition : Option[Expression] = None
//  val leftKeys = joinKeys(0)
//  val rightKeys = joinKeys(1)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def requiredChildDistribution: Seq[Distribution] =
    joinKeys.map(joinKey => HyperCubeDistribution(joinKey))

  protected override def doExecute() : RDD[InternalRow] = {
//    nodes.zipWithIndex.reduceLeft((left, right) => {
//      left match {
//        case (l: SparkPlan, lIndex: Int)
//      }
//
//    }
    for (i <- 1 to nodes.size) {
      
    }
  }



}

case class HyperCubePairwiseJoin(leftKeys: Seq[Expression],
                                 rightKeys: Seq[Expression],
                                 joinType: JoinType,
                                 buildSide: BuildSide,
                                 condition: Option[Expression],
                                 left: SparkPlan,
                                 right: SparkPlan)
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

    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      join(streamIter, hashed, numOutputRows)
    }
  }
  }


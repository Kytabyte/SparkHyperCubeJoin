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

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{MultaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics



/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class HyperCubeJoinExec(mapKeys: Seq[Seq[Expression]],
                             logicalPlan: LogicalPlan,
                             // planIndexMap: mutable.HashMap[LogicalPlan, Int],
                             nodes: Seq[SparkPlan])
  extends MultaryExecNode with PredicateHelper {

  override def output: Seq[Attribute] = nodes.map(_.output).reduce(_ ++ _)

  override def requiredChildDistribution: Seq[Distribution] =
    mapKeys.map(mapKey => HyperCubeDistribution(mapKey))

  lazy val rdds : Seq[RDD[InternalRow]] = nodes.map(_.execute())
  val planIndexMap: mutable.HashMap[LogicalPlan, Int] = new mutable.HashMap()
  var index: Int = 0

  private def extractInnerJoins(plan: LogicalPlan): Unit = {

    plan match {
      case Join(left, right, _: InnerLike, Some(cond)) =>
        extractInnerJoins(left)
        extractInnerJoins(right)
      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond)))
        if projectList.forall(_.isInstanceOf[Attribute]) => {
        extractInnerJoins(j)
      }
      case _ =>
        planIndexMap.put(plan, index)
        index += 1
    }
  }

  def prepareHashJoinExec(plan: LogicalPlan) : SparkPlan = {
    plan match {
      case j @ Join(left, right, _: InnerLike, condition) =>
        val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)

        val joinKeys = predicates.flatMap {
          case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
          case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
          case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
          case _ => None
        }
        val otherPredicates = predicates.filterNot {
          case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => false
          case EqualTo(l, r) =>
            canEvaluate(l, left) && canEvaluate(r, right) ||
              canEvaluate(l, right) && canEvaluate(r, left)
          case _ => false
        }

        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")

        val leftRDDIndex = planIndexMap.getOrElse(left, -1)
        val rightRDDIndex = planIndexMap.getOrElse(right, -1)

        val leftPlan = if (leftRDDIndex == -1) {
          prepareHashJoinExec(left)
        } else {
          children(leftRDDIndex)
        }

        val rightPlan = if (rightRDDIndex == -1) {
          prepareHashJoinExec(right)
        } else {
          children(rightRDDIndex)
        }

        val leftRDD = if (leftRDDIndex == -1) {
          None
        } else {
          Some(rdds(leftRDDIndex))
        }

        val rightRDD = if (rightRDDIndex == -1) {
          None
        } else {
          Some(rdds(rightRDDIndex))
        }

        HyperCubeHashJoinExec(leftKeys, rightKeys, Inner, BuildRight,
          otherPredicates.reduceOption(And), leftPlan, rightPlan, leftRDD, rightRDD)

      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(_))) =>
        prepareHashJoinExec(j)

      case _ => prepareHashJoinExec(plan)

    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    extractInnerJoins(logicalPlan)
    val execPlan = prepareHashJoinExec(logicalPlan)
    execPlan.execute()
//    rdds(0)
  }
}


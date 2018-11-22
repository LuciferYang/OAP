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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.oap.adapter.FileSourceScanExecAdapter

object OapFileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    def tryOptimize(head: SparkPlan): Seq[SparkPlan] = {
      head match {
        // Project -> Filter -> Scan
        case ProjectExec(projectList, FilterExec(condition,
          FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier))) =>
          val scan = FileSourceScanExecAdapter.createOptimizedFileSourceScanExec(relation,
            output, outputSchema, partitionFilters, dataFilters, tableIdentifier)
          execution.ProjectExec(projectList, execution.FilterExec(condition, scan)) :: Nil
        // Project -> Scan
        case ProjectExec(projectList,
          FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier)) =>
          val scan = FileSourceScanExecAdapter.createOptimizedFileSourceScanExec(relation,
            output, outputSchema, partitionFilters, dataFilters, tableIdentifier)
          execution.ProjectExec(projectList, scan) :: Nil
        // Filter -> Scan
        case FilterExec(condition, FileSourceScanExec(relation, output, outputSchema,
          partitionFilters, dataFilters, tableIdentifier)) =>
          val scan = FileSourceScanExecAdapter.createOptimizedFileSourceScanExec(relation,
            output, outputSchema, partitionFilters, dataFilters, tableIdentifier)
          execution.FilterExec(condition, scan) :: Nil
        // Scan
        case FileSourceScanExec(relation, output, outputSchema, partitionFilters,
          dataFilters, tableIdentifier) =>
          FileSourceScanExecAdapter.createOptimizedFileSourceScanExec(relation,
            output, outputSchema, partitionFilters, dataFilters, tableIdentifier) :: Nil
        case _ => throw new OapException(s"Unsupport plan mode $head")
      }
    }

    plan match {
      case PhysicalOperation(_, _, LogicalRelation(_: HadoopFsRelation, _, _)) =>
        FileSourceStrategy(plan).headOption match {
          case Some(head) => tryOptimize(head)
          case _ => Nil
        }
      case _ => Nil
    }
  }
}

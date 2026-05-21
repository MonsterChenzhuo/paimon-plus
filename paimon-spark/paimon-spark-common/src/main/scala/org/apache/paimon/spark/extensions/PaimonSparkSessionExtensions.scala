/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.extensions

import org.apache.paimon.spark.catalyst.analysis.{PaimonAnalysis, PaimonDeleteTable, PaimonFunctionResolver, PaimonIncompatibleResolutionRules, PaimonMergeInto, PaimonPostHocResolutionRules, PaimonProcedureResolver, PaimonUpdateTable, PaimonViewResolver, ReplacePaimonFunctions, RewriteUpsertTable}
import org.apache.paimon.spark.catalyst.optimizer.{MergePaimonScalarSubqueries, OptimizeMetadataOnlyDeleteFromPaimonTable}
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions
import org.apache.paimon.spark.commands.BucketExpression
import org.apache.paimon.spark.execution.{OldCompatibleStrategy, PaimonStrategy}
import org.apache.paimon.spark.execution.adaptive.DisableUnnecessaryPaimonBucketedScan

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.paimon.shims.SparkShimLoader

/** Spark session extension to extends the syntax and adds the rules. */
class PaimonSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => SparkShimLoader.shim.createSparkParser(parser) }

    // analyzer extensions
    extensions.injectResolutionRule(spark => new PaimonAnalysis(spark))
    extensions.injectResolutionRule(spark => PaimonProcedureResolver(spark))
    extensions.injectResolutionRule(spark => PaimonViewResolver(spark))
    extensions.injectResolutionRule(spark => PaimonFunctionResolver(spark))
    extensions.injectResolutionRule(spark => SparkShimLoader.shim.createCustomResolution(spark))
    extensions.injectResolutionRule(spark => PaimonIncompatibleResolutionRules(spark))
    extensions.injectResolutionRule(spark => RewriteUpsertTable(spark))

    extensions.injectPostHocResolutionRule(spark => ReplacePaimonFunctions(spark))
    extensions.injectPostHocResolutionRule(spark => PaimonPostHocResolutionRules(spark))
    extensions.injectPostHocResolutionRule(spark => NativeIODiagnosticsInstallRule(spark))

    extensions.injectPostHocResolutionRule(_ => PaimonUpdateTable)
    extensions.injectPostHocResolutionRule(_ => PaimonDeleteTable)
    extensions.injectPostHocResolutionRule(spark => PaimonMergeInto(spark))

    // table function extensions
    PaimonTableValuedFunctions.supportedFnNames.foreach {
      fnName =>
        extensions.injectTableFunction(
          PaimonTableValuedFunctions.getTableValueFunctionInjection(fnName))
    }

    // scalar function extensions
    BucketExpression.supportedFnNames.foreach {
      fnName => extensions.injectFunction(BucketExpression.getFunctionInjection(fnName))
    }

    // optimization rules
    extensions.injectOptimizerRule(_ => OptimizeMetadataOnlyDeleteFromPaimonTable)
    extensions.injectOptimizerRule(_ => MergePaimonScalarSubqueries)

    // planner extensions
    extensions.injectPlannerStrategy(spark => PaimonStrategy(spark))
    // old compatible
    extensions.injectPlannerStrategy(spark => OldCompatibleStrategy(spark))

    // query stage preparation
    extensions.injectQueryStagePrepRule(_ => DisableUnnecessaryPaimonBucketedScan)
  }
}

private object NativeIODiagnosticsInstallRule {

  private val NativeIOEnabledKey = "spark.paimon.native-io.enabled"
  private val NativeIOUIEnabledKey = "spark.paimon.native-io.ui.enabled"
  private val DiagnosticsClass = "org.apache.paimon.spark.nativeio.diagnostics.NativeIODiagnostics"

  def apply(spark: SparkSession): Rule[LogicalPlan] = {
    new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        installIfEnabled(spark)
        plan
      }
    }
  }

  private def installIfEnabled(spark: SparkSession): Unit = {
    if (!confBoolean(spark, NativeIOEnabledKey, defaultValue = false) ||
        !confBoolean(spark, NativeIOUIEnabledKey, defaultValue = true)) {
      return
    }

    try {
      val diagnostics =
        Class.forName(DiagnosticsClass, true, Thread.currentThread().getContextClassLoader)
      diagnostics
        .getMethod("install", classOf[SparkSession])
        .invoke(null, spark)
    } catch {
      case _: Throwable =>
    }
  }

  private def confBoolean(
      spark: SparkSession,
      key: String,
      defaultValue: Boolean): Boolean = {
    val default = spark.sparkContext.getConf.get(key, defaultValue.toString)
    spark.sessionState.conf.getConfString(key, default).equalsIgnoreCase("true")
  }
}

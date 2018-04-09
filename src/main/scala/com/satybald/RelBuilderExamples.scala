package com.satybald

import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Programs, RelBuilder}

class RelBuilderExamples {
  val config = FoodMartConfig.config()
  val builder: RelBuilder = RelBuilder.create(config)

  def unitSalesSum(builder: RelBuilder) = {
    builder.scan("public.sales_fact_1998")
    .scan("public.customer")
    .join(JoinRelType.INNER, "customer_id")
    .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("city"), builder.literal("Albany")))
    .sum(false, "sales", builder.field("unit_sales"))
  }

  def unitSalesSumByMonth(builder: RelBuilder) = {
    builder.scan("public.sales_fact_1998")
      .scan("public.time_by_day")
      .join(JoinRelType.INNER, "time_id")
      .sum(false, "sales", builder.field("unit_sales"))
  }

  def top5UnitSalesSum(builder: RelBuilder) = {
    builder.scan("public.sales_fact_1998")
      .scan("public.customer")
      .join(JoinRelType.INNER, "customer_id")
      .aggregate(
        builder.groupKey("lname"),
        builder.sum(false, "sales", builder.field("unit_sales")))
      .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("city"), builder.literal("Albany")))
      .sortLimit(0, 5, builder.field("lname"))
  }

  def getUnitSalesSum(): RelNode = {
    unitSalesSum(builder)
    builder.build
  }

  def getUnitSalesSumByMonth(): RelNode = {
    unitSalesSumByMonth(builder)
    builder.build
  }

  def getTop5ByMonth(): RelNode = {
    top5UnitSalesSum(builder)
    builder.build
  }
}

object FoodMartConfig {

  def getSchema(rootSchema: SchemaPlus) = {
    val dataSource = JdbcSchema.dataSource("jdbc:postgresql://localhost/foodmart", "org.postgresql.Driver", "foodmart", "foodmart")
    rootSchema.add("foodmart", JdbcSchema.create(rootSchema, "foodmart", dataSource, null, null))
  }

  def config(): FrameworkConfig = {
    val internalSchema = Frameworks.createRootSchema(true)
    val rootSchema = getSchema(internalSchema)
    Frameworks
      .newConfigBuilder
      .parserConfig(SqlParser.Config.DEFAULT)
      .defaultSchema(rootSchema)
      .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
      .build()
  }
}

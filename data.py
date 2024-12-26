from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, dayofmonth, lit
import pyspark.sql.functions as psf

# 初始化SparkSession
spark = SparkSession.builder.appName("FundFlowPrediction").getOrCreate()

# 读取数据
daily_data = spark.read.csv("file:///home/zhangyilu/downloads/user_balance_table.csv", inferSchema=True, header=True)

# 转换日期格式
daily_data = daily_data.withColumn("report_date", to_date("report_date", "yyyyMMdd"))

# 选择需要的列
daily_data = daily_data.select("report_date", "total_purchase_amt", "total_redeem_amt")

# 聚合数据
user_data = daily_data.groupBy("report_date") \
    .agg(F.sum("total_purchase_amt").alias("total_purchase_amt"), \
         F.sum("total_redeem_amt").alias("total_redeem_amt"))

# 特征工程
user_data = user_data.withColumn("day_of_month", F.dayofmonth("report_date"))
user_data = user_data.withColumn("day_of_week", F.dayofweek("report_date"))

# 处理缺失值
user_data = user_data.na.fill({'total_purchase_amt': 0, 'total_redeem_amt': 0})

# 特征向量化
assembler = VectorAssembler(inputCols=["day_of_month", "day_of_week"], outputCol="features")
user_data = assembler.transform(user_data)

# 分割数据集
train_data = user_data.filter(user_data["report_date"] < "2014-09-01")
test_data = user_data.filter(user_data["report_date"] >= "2014-09-01")

# 训练随机森林模型
rf_model_purchase = RandomForestRegressor(featuresCol="features", labelCol="total_purchase_amt")
rf_model_redeem = RandomForestRegressor(featuresCol="features", labelCol="total_redeem_amt")
purchase_fit = rf_model_purchase.fit(train_data)
redeem_fit = rf_model_redeem.fit(train_data)

# 创建未来日期的特征数据
future_dates = [{"date": f"2014-09-{i+1}"} for i in range(30)]
future_data = spark.createDataFrame(future_dates)
future_data = future_data.withColumn("day_of_month", F.dayofmonth("date"))
future_data = future_data.withColumn("day_of_week", F.dayofweek("date"))
future_data = assembler.transform(future_data)

# 预测
predictions_purchase = purchase_fit.transform(future_data)
predictions_redeem = redeem_fit.transform(future_data)


results_purchase = predictions_purchase.select(F.col("date").alias("report_date"), (F.col("prediction") * 1.7).cast("bigint").alias("purchase"))
results_redeem = predictions_redeem.select(F.col("date").alias("report_date"), (F.col("prediction") * 1.7).cast("bigint").alias("redeem"))

# 合并两个结果集
results = results_purchase.join(results_redeem, on="report_date", how="inner")
results = results.withColumn("report_date", F.date_format("report_date", "yyyyMMdd"))

# 按日期排序
results = results.orderBy("report_date")

# 输出到CSV
results.write.csv('file:///home/zhangyilu/bigdata/task4/tc_comp_predict_table.csv', header=True, mode='overwrite')
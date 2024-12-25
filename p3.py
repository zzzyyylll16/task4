from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("AverageBalanceByCity") \
    .getOrCreate()

user_profiles = spark.read.csv("file:///home/zhangyilu/downloads/user_profile_table.csv", header=True, inferSchema=True)
user_balances = spark.read.csv("file:///home/zhangyilu/downloads/user_balance_table.csv", header=True, inferSchema=True)
from pyspark.sql.functions import col, to_date

# 转换report_date列为日期类型，并过滤出2014年3月1日的数据
user_balances = user_balances.withColumn("report_date", to_date(col("report_date"), "yyyyMMdd")) \
                               .filter(col("report_date") == "2014-03-01")
joined_data = user_balances.join(user_profiles, user_balances.user_id == user_profiles.user_id, "inner")

average_balance_by_city = joined_data.groupBy("city") \
                                   .agg({"tBalance": "avg"}) \
                                   .withColumnRenamed("avg(tBalance)", "average_balance")
result = average_balance_by_city.orderBy(col("average_balance").desc()) \
                               .select("city", "average_balance")

result.show()
spark.stop()                                             
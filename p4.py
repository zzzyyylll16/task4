from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window
# 初始化SparkSession
spark = SparkSession.builder.appName("TopUsersByTraffic").getOrCreate()

# 读取数据
user_profile_df = spark.read.csv("file:///home/zhangyilu/downloads/user_profile_table.csv", header=True, inferSchema=True)
user_balance_df = spark.read.csv("file:///home/zhangyilu/downloads/user_balance_table.csv", header=True, inferSchema=True)
user_profile_df = user_profile_df.alias("user_profile")

# 过滤出2014年8月的数据
user_balance_august_df = user_balance_df.filter((user_balance_df.report_date >= "20140801") & (user_balance_df.report_date <= "20140831"))

# 计算每个用户在2014年8月的总流量
user_traffic_df = user_balance_august_df.groupBy("user_id").agg(sum("total_purchase_amt").alias("total_purchase_amt"), 
                                                            sum("total_redeem_amt").alias("total_redeem_amt"))
user_traffic_df = user_traffic_df.withColumn("total_traffic", col("total_purchase_amt") + col("total_redeem_amt"))
user_traffic_df = user_traffic_df.alias("user_traffic")
#user_traffic_df.show(20)
user_traffic_with_city_df = user_traffic_df.join(user_profile_df, user_traffic_df.user_id == user_profile_df.user_id, "inner") \
                                          .select(
                                              "user_traffic.user_id",  # 明确指定user_traffic中的user_id
                                              "user_traffic.total_traffic",
                                              "user_profile.city",     # 明确指定user_profile中的city
                                          )
#user_traffic_with_city_df.show(20)
# 定义窗口规范
windowSpec = Window.partitionBy("city").orderBy(col("total_traffic").desc())

# 使用窗口函数来获取每个城市的前3名
top_users_by_city_df = user_traffic_with_city_df.withColumn(
    "row_num", row_number().over(windowSpec)
).filter(col("row_num") <= 3).select("city", "user_id", "total_traffic").show(30)

# 停止SparkSession
spark.stop()
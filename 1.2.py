from pyspark import SparkContext
# 初始化SparkContext
sc = SparkContext(appName="ActiveUserAnalysis")

# 读取本地CSV文件
lines = sc.textFile("file:///home/zhangyilu/downloads/user_balance_table.csv")
header = lines.first()  # 获取第一行，即标题行
data = lines.filter(lambda line: line != header)
# 解析CSV文件，提取user_id, report_date
def parse_line(line):
    parts = line.split(',')
    user_id = parts[0]
    report_date = parts[1]
    # 检查用户当天是否活跃
    is_active = int(parts[5]) > 0 or int(parts[8]) > 0
    return (user_id, report_date, is_active)

# 转换为RDD并解析数据
user_activity_rdd = data.map(parse_line).filter(lambda x: x[2])

# 按用户ID分组，并计算每个用户在2014年8月的活跃天数
active_days_rdd = user_activity_rdd.filter(lambda x: x[1].startswith('201408')) \
                                   .map(lambda x: (x[0], 1)) \
                                   .reduceByKey(lambda a, b: a + b)

# 过滤出活跃天数至少为5天的用户
active_users_rdd = active_days_rdd.filter(lambda x: x[1] >= 5) \
                                   .map(lambda x: x[0])

# 计算活跃用户总数
active_user_count = active_users_rdd.distinct().count()

print(f"<活跃用户总数>{active_user_count}<活跃用户总数>")

# 停止SparkContext
sc.stop()
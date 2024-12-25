from pyspark import SparkContext
# 初始化SparkContext
sc = SparkContext(appName="UserBalanceAnalysis")

# 读取本地CSV文件
lines = sc.textFile("file:///home/zhangyilu/downloads/user_balance_table.csv")
header = lines.first()  # 获取第一行，即标题行
data = lines.filter(lambda line: line != header)
# 解析CSV文件，提取日期、资金流入和流出量
def parse_line(line):
    parts = line.split(',')
    date = parts[1].strip()
    try:
        purchase_amt = float(parts[4].strip() or 0) 
    except ValueError:
        purchase_amt = 0.0 
    try:
        redeem_amt = float(parts[8].strip() or 0)    # total_redeem_amt, default to 0 if missing
    except ValueError:
        redeem_amt = 0.0 
    return (date, purchase_amt, -redeem_amt)  # 资金流入和流出量

# 转换为RDD并去除空行
balance_rdd = data.map(parse_line).filter(lambda x: x[0] is not None)

# 分别计算资金流入和流出
inflow_rdd = balance_rdd.map(lambda x: (x[0], x[1])).reduceByKey(lambda a, b: a + b)
outflow_rdd = balance_rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda a, b: a + b)

# 合并流入和流出量
daily_balance = inflow_rdd.join(outflow_rdd).mapValues(lambda x: (x[0], -x[1]))

sorted_daily_balance = daily_balance.sortByKey()

# 收集结果并打印
results = sorted_daily_balance.collect()
for date, (inflow, outflow) in results:
    print(f"{date} {inflow} {outflow}")

# 停止SparkContext
sc.stop()
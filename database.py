import mysql.connector
import pymongo
from pymongo import MongoClient
import calendar
from datetime import datetime, timedelta
# 创建连接
#mysql实例
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="sakila"
)

# 创建mongodb连接
client = pymongo.MongoClient("mongodb://localhost:27017/")

def mysql_select(query):   

    # 创建游标
    cursor = connection.cursor()

    # 执行查询操作
    cursor.execute(f"{query}")

    # 获取查询结果
    results = cursor.fetchall()

    print(f"查询到的数据量：{len(results)}")

    # 关闭游标和连接
    cursor.close()
    connection.close()


def mongodb_init(year,month):
    # 获取数据库和集合
    db = client["test"]  # 替换为你的数据库名
    mongo_collection = db["api_visit"]  # 替换为你的集合名
    
    # 构建聚合管道
    pipeline = [
        {
         "$match": {
                "time": {
                    "$gt": f"{year}-{month:02d}-01 00:00:00",
                    "$lt": f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]} 23:59:59"
                }   
            }
        },
     {
            "$group": {
                "_id": "$user_id",
                "times": {
                    "$push": "$time"
                }
         }
     },
     {
            "$addFields": {
                "timesCount": { "$size": "$times" }
         }
     },
        {
            "$sort": {
                "timesCount": -1
            }
        }
    ]

    # 执行聚合查询

    result = mongo_collection.aggregate(pipeline, allowDiskUse=True)

    # 输出查询结果
    results = []
    for doc in result :
        results.append(doc)

    array_length = len(results)
    print(f"{year}-{month}月度活跃人数为：{array_length}")
    print("\n")


def mysql_distribution_info():
    #店铺已开通，余额大于0
    query = "SELECT FROM_UNIXTIME(last_time),eb_user.real_name,eb_user.uid,sale_balance,create_time from eb_distributor_info LEFT JOIN eb_user ON eb_distributor_info.uid = eb_user.uid WHERE eb_distributor_info.`status` = 1 and  eb_distributor_info.sale_balance > 0 ;"

    #店铺已开通
    #query = "SELECT FROM_UNIXTIME(last_time),eb_user.real_name,eb_user.uid,sale_balance,create_time from eb_distributor_info LEFT JOIN eb_user ON eb_distributor_info.uid = eb_user.uid WHERE eb_distributor_info.`status` = 1"

    mysql_select(query)

def run_user_request_info():
    year = 2022
    month = 9
    for _ in range (0,1):
        mongodb_init(year,month)
        if month == 12 :
            month = 1
            year = year + 1
        else :
            month = month + 1

def run_user_request_week_info():
    datetime_string = "2022-09-01 00:00:00"
    datetime_object = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    one_week_ago = datetime_object - timedelta(weeks=1)
    one_week_ago_string = one_week_ago.strftime("%Y-%m-%d %H:%M:%S")
   
    week_index = 1
    for _ in range (0,1):
        mongodb_init(year,month)
        if month == 12 :
            month = 1
            year = year + 1
        else :
            month = month + 1

def main():
    # mysq数据库
    #mysql()

    #mysql_distribution_info()

    run_user_request_info()


if __name__ == "__main__":
    main()

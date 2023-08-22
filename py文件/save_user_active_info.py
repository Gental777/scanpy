import mysql.connector
import pymongo
from pymongo import MongoClient
import calendar
from datetime import datetime, timedelta
import pandas
import time

#-----从mongodb表中读取用户访问日志，按日筛选插入到用户活跃表-----

insert_table = 'j_copy1'

# 创建数据库连接
def create_mysql_connection(host, user, password, database):
    return mysql.connector.connect(host=host, user=user, password=password, database=database)

# 创建MongoDB连接
def create_mongo_connection(host, port):
    return pymongo.MongoClient(f"mongodb://{host}:{port}/")

# 查询用户
def search_user(cursor):
    cursor.execute("SELECT * FROM eb_user")
    return cursor.fetchall()

# 从MongoDB获取用户活跃信息
def get_user_activity_dates(collection, start_date, end_date):
    pipeline = [
        {
            "$match": {
                "time": {
                    "$gte": start_date,
                    "$lt": end_date
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
    return collection.aggregate(pipeline, allowDiskUse=True)

# 初始化查询管道
def init_pipeline(start, end):
    return [
        {
            "$match": {
                "time": {
                    "$gte": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "$lt": end.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        },
        {
            "$group": {
                "_id": "$user_id"
            }
        }
    ]

#从user表查询用户注册时间
def select_user_add_time(cursor, uid):
    query = f"SELECT DATE_FORMAT(FROM_UNIXTIME(add_time), '%Y-%m-%d %H:%i:%s') AS add_time FROM sakila.eb_user WHERE uid = {uid}"
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return result[0]
    return None
    
#插入数据到用户活跃表
def insert_info_to_j(connection, cursor, result, date):
    if not result:
        return
    values = []
    for doc in result:
        uid = doc["_id"]
        #查询用户注册日期
        add_time = select_user_add_time(cursor, uid)
        if add_time:
            values.append((uid, date, add_time))

    if values:
        query = f"INSERT INTO {insert_table} (uid, date, add_time) VALUES (%s, %s, %s)"
        cursor.executemany(query, values)
        connection.commit()
        print(f"-----插入{date}数据-----")


# 主函数
def main():
    
    #创建mongoDB链接
    client = create_mongo_connection('localhost',27017)
    db = client['test']
    mongo_collection = db['api_visit']


    # 创建MySQL连接
    connection = create_mysql_connection("localhost", "root", "123456", "world")
    cursor = connection.cursor()

    # 获取数据库和集合
    db = client["test"]  
    mongo_collection = db["api_visit"]  

    # 设置查询的日期范围
    start_date = datetime(2022, 9, 1)
    stop_date = datetime(2023,8,4)

    while start_date < stop_date :

        end_date = start_date + timedelta(days=1)
        # 构建聚合管道
        pipeline = init_pipeline(start_date,end_date)

        # 执行聚合查询
        result = mongo_collection.aggregate(pipeline, allowDiskUse=True)

        #for doc in result :
        insert_info_to_j(connection,cursor,result,start_date.strftime("%Y-%m-%d %H:%M:%S"))

        start_date = end_date
    
    
    # 关闭数据库连接
    connection.close()
    client.close()


if __name__ == "__main__":
    main()
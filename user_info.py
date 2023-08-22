import mysql.connector
import pymongo
from pymongo import MongoClient
import calendar
from datetime import datetime, timedelta
import pandas
import time


# 创建mongodb连接
client = pymongo.MongoClient("mongodb://localhost:27017/")
# 创建连接
#mysql实例
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="sakila"
)

connection2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="world"
)

# 创建游标
cursor = connection.cursor()
cursor2 = connection2.cursor()


def search_user() :
    cursor.execute("select * from eb_user")
    results = cursor.fetchall
    return results

def compare_user_active_time() :
    results  = search_user()

    for obj in results :
        # 使用datetime模块将时间戳转换为datetime对象
        dt_object = datetime.datetime.fromtimestamp(obj["add_time"])
        # 格式化为字符串
        formatted_time = dt_object.strftime('%Y-%m-%d %H:%M:%S')

def search_user_last_login(date_string,type) :
    # 获取数据库和集合
    db = client["test"]  # 替换为你的数据库名
    mongo_collection = db["api_visit"]  # 替换为你的集合名
    # 构建聚合管道
    pipeline = [
        {
         "$match": {
                "time": {
                    "$gt": "nowdate",
                    "$lt":"nextdate"
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

def deal_next_search_date(date_string,type) :
    #if type == 1 :
    return 1

def init_pipeline(start,end) :
    pipeline = [
            {
            "$match": {
                    "time": {
                        "$gte": start.strftime("%Y-%m-%d %H:%M:%S"),
                        "$lt":end.strftime("%Y-%m-%d %H:%M:%S")
                    }   
                }
            },
         {
                "$group": {
                    "_id": "$user_id",
            }
        },
        ]  
    return pipeline  

def save_user_active_info() :
    # 设置查询的日期范围
    # 获取数据库和集合
    db = client["test"]  
    mongo_collection = db["api_visit"]  
    start_date = datetime(2022, 9, 1)

    while start_date < datetime.now() :

        end_date = start_date + timedelta(days=1)
        # 构建聚合管道
        pipeline = init_pipeline(start_date,end_date)

        # 执行聚合查询
        result = mongo_collection.aggregate(pipeline, allowDiskUse=True)

        #for doc in result :
        insert_info_to_j(result,start_date.strftime("%Y-%m-%d %H:%M:%S"))

        start_date = end_date

def insert_info_to_j(result,date) :
    
    values = ""

    for doc in result :
 
        uid = doc["_id"]
        values = values + f"('{uid}','{date}'),"

    #无数据直接结束
    if values == "" :
        return 1

    values = values[:-1]

    query = f"insert into j (uid,date) values {values}"
    #query = "insert into  j (uid,date) values (1,'2023-01-01'),(2,'2023-01-01')"
    cursor2.execute(query)
    connection2.commit()
    print(f"-----插入{date}数据-----")

    excute_result = cursor2.fetchall()

    for obj in excute_result :
        print(obj)

def count_common_element(arr1, arr2):

    counts = 0
    for item1 in arr1:
        for item2 in arr2:
            if item1[0] == item2[0] :
                counts += 1
    return counts

def cacu_user_retain() :
    now_week = 1

    table_titles = []
    tables_datas = []
    table_registers = []
    for _ in range(0,52) :
        if now_week > 52 :
            break 
        #查询第一周数据
        query = f"SELECT DISTINCT a.uid FROM(SELECT WEEK(date) as week_num,WEEK(add_time) as add_week,YEAR(date) as a_year , YEAR(add_time) as b_year ,date,add_time,uid  FROM `j`) as a WHERE a.a_year = a.b_year AND a.week_num = a.add_week AND a.week_num = {now_week}"  
        cursor2.execute(query)
        result1 = cursor2.fetchall()

        query = f"SELECT uid FROM j WHERE WEEK(date) = {now_week+1} GROUP BY uid;"  
        cursor2.execute(query)
        result2 = cursor2.fetchall()

        common_count = count_common_element(result1,result2)

        table_titles.append(f"2023第{now_week}周")
        table_registers.append(len(result1))
        tables_datas.append(common_count)

        print(F"第{now_week}周，注册{len(result1)},次周留存{common_count}")
        now_week += 1
    
    df = pandas.DataFrame(
        {"日期":table_titles,
        "注册人数":table_registers,
        "次周留存":tables_datas
        }
        )
    # 创建一个Excel写入对象
    excel_writer = pandas.ExcelWriter('2023liucun.xlsx', engine='xlsxwriter')

    # 将DataFrame写入Excel文件
    df.to_excel(excel_writer, sheet_name='Sheet1', index=False)

    # 关闭Excel写入对象
    excel_writer.save()

    print("Excel文件已生成")
       



#
def insert_day_user_info():
    #从活跃表中筛选出日期
    query1 = "SELECT date,COUNT(date) as counts FROM j GROUP BY date;"
    cursor2.execute(query1)
    result1 = cursor2.fetchall()
    
    #按照日期，以天为单位，查询留存数据
    for obj in result1 :
        insert_to_datebase(obj[0],obj[1])

def insert_to_datebase(date,activity_counts) :
    
    register_query = f"SELECT date, uid, DATE(add_time) AS create_at FROM j WHERE DATE(add_time) = date AND date = '{date}'"

    cursor2.execute(register_query)
    #获取此日期新注册用户
    register_users = cursor2.fetchall()

    for obj in register_users :
        liucun_data = [0,0,0,0,0,0,0,0,0,0,0,0]
        #查询留存
        day1_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 1 DAY) AND DATE('{obj[0]}' + INTERVAL 1 DAY) THEN 'day1' "
        day2_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 2 DAY) AND DATE('{obj[0]}' + INTERVAL 2 DAY) THEN 'day2' "
        day3_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 3 DAY) AND DATE('{obj[0]}' + INTERVAL 3 DAY) THEN 'day3' "
        day4_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 4 DAY) AND DATE('{obj[0]}' + INTERVAL 4 DAY) THEN 'day3' "
        day5_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 5 DAY) AND DATE('{obj[0]}' + INTERVAL 5 DAY) THEN 'day3' "
        day6_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 6 DAY) AND DATE('{obj[0]}' + INTERVAL 6 DAY) THEN 'day3' "
        day7_s =  f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 7 DAY) AND DATE('{obj[0]}' + INTERVAL 7 DAY) THEN 'day7' "
        day15_s = f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 8 DAY) AND DATE('{obj[0]}' + INTERVAL 15 DAY) THEN 'day15' "
        day30_s = f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 16 DAY) AND DATE('{obj[0]}' + INTERVAL 30 DAY) THEN 'day30' " 
        day60_s = f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 31 DAY) AND DATE('{obj[0]}' + INTERVAL 60 DAY) THEN 'day60' " 
        day120_s = f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 61 DAY) AND DATE('{obj[0]}' + INTERVAL 120 DAY) THEN 'day120' "
        day180_s = f"WHEN DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 121 DAY) AND DATE('{obj[0]}' + INTERVAL 180 DAY) THEN 'day180' "
        condition1 = f"ELSE 'Other' END AS time_interval,  COUNT(*) AS count FROM j "
        condition2 = f"WHERE DATE(date) BETWEEN DATE('{obj[0]}' + INTERVAL 1 DAY) AND DATE('{obj[0]}' + INTERVAL 500 DAY) "							 
        condition3 = f"AND uid = '{obj[1]}' GROUP BY time_interval"

        query_liucun = "SELECT CASE " +day1_s+day2_s+day3_s+day4_s+day5_s+day6_s+day7_s+day15_s+day30_s+day60_s+day120_s+day180_s+condition1+condition2+condition3
        cursor2.execute(query_liucun)

        result = cursor2.fetchall()

        for obj1 in result : 
            if obj1[0] == 'day1' :
                liucun_data[0] = 1
            elif obj1[0] == 'day2' :
                liucun_data[1] = 1
            elif obj1[0] == 'day3' :
                liucun_data[2] = 1
            elif obj1[0] == 'day4' :
                liucun_data[3] = 1
            elif obj1[0] == 'day5' :
                liucun_data[4] = 1
            elif obj1[0] == 'day6' :
                liucun_data[5] = 1
            elif obj1[0] == 'day7' :
                liucun_data[6] = 1
            elif obj1[0] == 'day15' :
                liucun_data[7] = 1
            elif obj1[0] == 'day30' :
                liucun_data[8] = 1
            elif obj1[0] == 'day60' :
                liucun_data[9] = 1
            elif obj1[0] == 'day120' :
                liucun_data[10] = 1
            elif obj1[0] == 'day180' :
                liucun_data[11] = 1

        save_query = "insert into user_liucun (uid,day1,day2,day3,day4,day5,day6,day7,day14,day30,day60,day120,day180,date,day_activity_counts )"
        save_query += f" values({obj[1]},{liucun_data[0]},{liucun_data[1]},{liucun_data[2]},{liucun_data[3]},{liucun_data[4]},{liucun_data[5]},{liucun_data[6]},{liucun_data[7]},{liucun_data[8]},{liucun_data[9]},{liucun_data[10]},{liucun_data[11]},"    
        save_query += f"'{obj[0]}',{activity_counts})"
        print("插入成功！")
        cursor2.execute(save_query)
        connection2.commit()
def main():
    #save_user_active_info()
    #compare_active_user(1)
    #cacu_user_retain()
    #insert_to_datebase("2022-10-10",1000)
    insert_day_user_info()

if __name__ == "__main__":
    main()
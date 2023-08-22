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

# 创建游标
cursor = connection.cursor()

def mongodb_init(nowdate,nextdate,index):
    print(f"第{index}周数据查询中...")
    excute_time = datetime.now().timestamp()
    # 获取数据库和集合
    db = client["test"]  # 替换为你的数据库名
    mongo_collection = db["api_visit"]  # 替换为你的集合名
    
    # 构建聚合管道
    pipeline = [
        {
         "$match": {
                "time": {
                    "$gt": nowdate,
                    "$lt":nextdate
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
    user_info = {}
    user_registers = []
    for doc in result :
        results.append(doc)
        user_info = compare_active_user(doc["_id"])
        user_registers.append(caculate_user_register_time(nowdate,user_info["add_time"]))
        
    
    array_length = len(results)
    use_times = (datetime.now().timestamp() - excute_time)
    print(f"共耗时{round(use_times,2)}s")
    #print(f"用户注册时间：{user_registers}")
    print("\n")
    return [array_length,user_registers]



def caculate_user_register_time(start_date,add_time):

    start = translateStringToDate(start_date)
    add = translateStringToDate(add_time)

    type = 1
    if start.timestamp() - add.timestamp() <= 0.0 :
        type = 1
        
    elif start.timestamp() - add.timestamp() <= 60*60*24*7.0 :
        #大于查询时间的0-7日内注册
        type = 2
    elif (start.timestamp() - add.timestamp() <=  60*60*24*14.0) & (start.timestamp() - add.timestamp() > 60*60*24*7.0):
        #大于查询时间的七天内 7-14日内注册
        type = 3   
    elif (start.timestamp() - add.timestamp() <=  60*60*24*30.0) & (start.timestamp() - add.timestamp() > 60*60*24*14.0):
        #大于查询时间的七天内 14-30日内注册
        type = 4    
    elif (start.timestamp() - add.timestamp() <=  60*60*24*60.0) & (start.timestamp() - add.timestamp() > 60*60*24*30.0) :
        #大于查询时间的七天内 30-60日内注册
        type = 5  
    elif (start.timestamp() - add.timestamp() <=  60*60*24*180.0) & (start.timestamp() - add.timestamp() > 60*60*24*60.0):
        #大于查询时间的  60-180日前注册
        type = 6    
    elif (start.timestamp() - add.timestamp() <=  60*60*24*365.0) & (start.timestamp() - add.timestamp() > 60*60*24*180.0) :
        #大于查询时间的  180前注册
        type = 7
    elif  start.timestamp() - add.timestamp() >  60*60*24*365.0 :
        type = 8                  
    return type

def translateDateToString(date) :

    return date.strftime("%Y-%m-%d %H:%M:%S")

def translateStringToDate(date_string) :

    return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

def compare_active_user(uid):

    query = f"SELECT uid,FROM_UNIXTIME(add_time) as add_time,FROM_UNIXTIME(last_time) as last_time FROM `eb_user` WHERE uid ={uid}"
    result = mysql_select(query)
    return result


def mysql_select(query):   

    #print(query)


    # 执行查询操作
    cursor.execute(f"{query}")

    # 获取查询结果
    results = cursor.fetchall()

    result_data = {}
    for obj in results :
        
        result_data = {"uid":obj[0],
        "add_time":translateDateToString(obj[1]),
        "last_time":translateDateToString(obj[2])}

    #print(f"查询到的数据：{result_data}")

    # 关闭游标和连接
    #cursor.close()
    #connection.close()
    return result_data

def run_user_request_week_info():
    datetime_string = "2022-09-01 00:00:00"
    week_index = 1
    table_dates = []
    table_counts = []
    #初始化8个日期分类数组
    table_user_date_types = [[],[],[],[],[],[],[],[]]
    for _ in range (0,60):

        #将string转换成date类型
        datetime_object = translateStringToDate(datetime_string)
        #计算一周后时间对象
        one_week_ago = datetime_object + timedelta(weeks=1)
        #将一周后时间对象转换成string
        one_week_ago_string = translateDateToString(one_week_ago)
        #大于当前时间后停止
        if one_week_ago > datetime.now() :
            break
        else :
            #请求数据
            now_data = mongodb_init(datetime_string,one_week_ago_string,week_index)
            #当前时间段的用户数
            table_counts.append(now_data[0])

            aa = datetime_string.split(" ")[0]
            bb = one_week_ago_string.split(" ")[0]
            table_dates.append(f"{aa} ~ {bb} 第{week_index}周")
            #赋值到下一周
            datetime_string = one_week_ago_string
            week_index += 1

            #查询用户注册时间，按分类计数
            date_sorts = sort_user_register_date(now_data[1])

            #遍历插入日期分类数组数据
            for i in range(0,8) :
                
                sort_array = table_user_date_types[i]
     
                sort_array.append(date_sorts[i])

    
    # 将数据转换为DataFrame（表格）
    print("excel文件写入中...")

    df = pandas.DataFrame(
        {"日期":table_dates,
        "数据":table_counts,
        "新注册":table_user_date_types[0],
        "(7日内)":table_user_date_types[1],
        "(7-14天)":table_user_date_types[2],
        "(14-30天)":table_user_date_types[3],
        "(30-60天)":table_user_date_types[4],
        "(60-180天)":table_user_date_types[5],
        "(180-365天)":table_user_date_types[6],
        "一年前":table_user_date_types[7]    }
        )

    # 创建一个Excel写入对象
    excel_writer = pandas.ExcelWriter('weekdata2.xlsx', engine='xlsxwriter')

    # 将DataFrame写入Excel文件
    df.to_excel(excel_writer, sheet_name='Sheet1', index=False)

    # 关闭Excel写入对象
    excel_writer.save()

    print("Excel文件已生成")

def sort_user_register_date(datas) :
 
    types = [0,0,0,0,0,0,0,0]

    for index in range(0,len(datas)) :
        #获取时间分类type
        type = datas[index]

        #根据type，更新指定type类型计数
        types[type-1] = types[type-1] +1

    return types

def main():

    run_user_request_week_info()
    #compare_active_user(1)



    

if __name__ == "__main__":
    main()

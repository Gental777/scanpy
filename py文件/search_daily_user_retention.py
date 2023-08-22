import mysql.connector
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd

#-----查询用户留存数据，导出excel表格-----

# 创建数据库连接
def create_mysql_connection(host, user, password, database):
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


# 统计公共元素个数
def count_common_element(arr1, arr2):
    return sum(1 for item1 in arr1 for item2 in arr2 if item1[0] == item2[0])


#查询liucun表中日期
def search_liucun_date(cursor) :
    query1 = "SELECT DATE_FORMAT(date, '%Y-%m-%d') AS date,COUNT(date) as counts FROM user_liucun2 GROUP BY date;"
    cursor.execute(query1)
    result1 = cursor.fetchall()
    return result1

#用户留存数据按日汇总
def daily_retain_data(cursor,date) :
    query = f"SELECT SUM(day1) as day1 ,SUM(day2) as day2 ,SUM(day3) as day3 ,SUM(day4) as day4 ,SUM(day5) as day5 ,SUM(day6) as day6 ,SUM(day7) as day7 ,SUM(day14) as day14 ,SUM(day30) as day30 ,SUM(day60) as day60 ,SUM(day120) as day120 ,SUM(day180) as day180 FROM user_liucun2 WHERE date = '{date}'"
    cursor.execute(query)
    result = cursor.fetchall()
    converted_result = [int(item) for item in result[0]]
    return converted_result


#转换成excel导出Dict类型
def transToExcelData(datas):
    date_string = datas[0]
    register_count:int = datas[1]
    original_retain_data = datas[2]

    excel_dict = {"日期":date_string,"注册人数":register_count}

    # 建立映射关系
    day_mapping2 = {
        0: '次日留存',
        1: '3日留存',
        2: '4日留存',
        3: '5日留存',
        4: '6日留存',
        5: '7日留存',
        6: '8日留存',
        7: '15日留存',
        8: '30日留存',
        9: '60日留存',
        10: '120日留存',
        11: '180日留存'
    }
    liucun_data = [[],[],[],[],[],[],[],[],[],[],[],[],]

    #重组array数据，
    for obj1 in original_retain_data:
        
        for i in range(0,12) :
            tem_retain_nums = obj1[i]
            liucun_data[i].append(f"{tem_retain_nums}")
    
    #生成字典
    for i in range(0,12) :
        key = f"{day_mapping2[i]}"
        excel_dict[key] = liucun_data[i]
           
    df = pd.DataFrame(excel_dict)

    return df

#导出excel
def out_excel(datas):
    df = transToExcelData(datas)

    # 创建一个Excel写入对象
    excel_writer = pd.ExcelWriter('liucun_percent.xlsx', engine='xlsxwriter')

    # 将DataFrame写入Excel文件
    df.to_excel(excel_writer, sheet_name='Sheet1', index=False)

    # 关闭Excel写入对象
    excel_writer.save()

    print("Excel文件已生成")

#查询并导出用户留存
def caculate_user_retian_data(connection2,cursor2):
    liucun_dates = search_liucun_date(cursor2)
    datas = [[],[],[]]

    #遍历日期，计算每日留存数据
    for items in liucun_dates :
        #日期
        day_date_string = items[0]
        #该日期新注册用户数
        day_register_count = items[1]
        #此日期留存数据
        daily_data = daily_retain_data(cursor2,day_date_string)

        #---------------只计算数值时，注释此段代码---------------
        #百分比留存数组
        daily_data2 = []

        #计算百分比留存
        for item in daily_data :
            daily_data2.append(item/day_register_count)

        #------------------------------------------------------

        datas[0].append(day_date_string)
        datas[1].append(day_register_count)
        datas[2].append(daily_data2)

    out_excel(datas)


# 主函数
def main():
   
    # 创建MySQL连接
    connection = create_mysql_connection("localhost", "root", "123456", "world")
    cursor = connection.cursor()
    
    caculate_user_retian_data(connection,cursor)
    # 关闭数据库连接
    connection.close()


if __name__ == "__main__":
    main()

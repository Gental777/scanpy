import mysql.connector
import pymongo
from pymongo import MongoClient

#-----查询用户活跃表，计算新用户留存数据，插入到留存表-----

# 创建数据库连接
def create_mysql_connection(host, user, password, database):
    return mysql.connector.connect(host=host, user=user, password=password, database=database)

# 查询用户
def search_user(cursor):
    cursor.execute("SELECT * FROM eb_user")
    return cursor.fetchall()


#生成mysql查询条件
def generate_case_conditions(date, uid):
        days =  [f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 1 DAY) AND DATE('{date}' + INTERVAL 1 DAY) THEN 'day1' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 2 DAY) AND DATE('{date}' + INTERVAL 2 DAY) THEN 'day2' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 3 DAY) AND DATE('{date}' + INTERVAL 3 DAY) THEN 'day3' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 4 DAY) AND DATE('{date}' + INTERVAL 4 DAY) THEN 'day4' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 5 DAY) AND DATE('{date}' + INTERVAL 5 DAY) THEN 'day5' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 6 DAY) AND DATE('{date}' + INTERVAL 6 DAY) THEN 'day6' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 7 DAY) AND DATE('{date}' + INTERVAL 7 DAY) THEN 'day7' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 8 DAY) AND DATE('{date}' + INTERVAL 15 DAY) THEN 'day15' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 16 DAY) AND DATE('{date}' + INTERVAL 30 DAY) THEN 'day30' " ,
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 31 DAY) AND DATE('{date}' + INTERVAL 60 DAY) THEN 'day60' " ,
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 61 DAY) AND DATE('{date}' + INTERVAL 120 DAY) THEN 'day120' ",
        f"WHEN DATE(date) BETWEEN DATE('{date}' + INTERVAL 121 DAY) AND DATE('{date}' + INTERVAL 180 DAY) THEN 'day180' "]
        condition1 = f"ELSE 'Other' END AS time_interval,  COUNT(*) AS count FROM j "
        condition2 = f"WHERE DATE(date) BETWEEN DATE('{date}' + INTERVAL 1 DAY) AND DATE('{date}' + INTERVAL 500 DAY) "							 
        condition3 = f"AND uid = '{uid}' GROUP BY time_interval"

        query_liucun = "SELECT CASE " + " ".join(days) + condition1 + condition2 + condition3

        return query_liucun   
         
#查询用户留存数据
def search_user_retain_data(connection,cursor, uid, date) :
        liucun_data = [0]*12
        #查询留存
        cursor.execute(generate_case_conditions(date,uid))

        result = cursor.fetchall()
        # 建立映射关系
        day_mapping = {
        'day1': 0,
        'day2': 1,
        'day3': 2,
        'day4': 3,
        'day5': 4,
        'day6': 5,
        'day7': 6,
        'day15': 7,
        'day30': 8,
        'day60': 9,
        'day120': 10,
        'day180': 11
        }

        for obj1 in result:
            interval = obj1[0]
            if interval in day_mapping:
                index = day_mapping[interval]
                liucun_data[index] = 1
        return liucun_data

# 插入留存数据到数据库
def insert_retention_data(connection,cursor, uid, retention_data, date, activity_counts):
    query = f"INSERT INTO user_liucun2 (uid, day1, day2, day3, day4, day5, day6, day7, day14, day30, day60, day120, day180, date, day_activity_counts) VALUES "
    query += f"({uid}, {','.join(map(str, retention_data))}, '{date}', {activity_counts})"

    cursor.execute(query)
    connection.commit()

# 查询活跃表日期及当日活跃人数
def select_active_date(connection,cursor) :
     #从活跃表中筛选出日期
    query1 = "SELECT date,COUNT(date) as counts FROM j GROUP BY date;"
    cursor.execute(query1)
    result1 = cursor.fetchall()
    return result1


#查询当日注册用户信息
def select_active_user_info(connection,cursor,date):
    register_query = f"SELECT date, uid, DATE(add_time) AS create_at FROM j WHERE DATE(add_time) = date AND date = '{date}'"
    cursor.execute(register_query)
    #获取此日期新注册用户
    register_users = cursor.fetchall()
    return register_users

# 统计公共元素个数
def count_common_element(arr1, arr2):
    return sum(1 for item1 in arr1 for item2 in arr2 if item1[0] == item2[0])    

# 处理用户留存，并插入数据库
def del_user_retain_function(connection2,cursor2):
    date_result = select_active_date(connection2,cursor2)
    #遍历日期内用户
    for obj in date_result :
        register_users = select_active_user_info(connection2,cursor2,obj[0])
        for user in register_users :
            # 插入留存数据
            user_retain_data = search_user_retain_data(connection2,cursor2,user[1],user[0])
            insert_retention_data(connection2,cursor2,user[1],user_retain_data,user[0],obj[1])
            print(f"{user[0]},{user[1]}数据插入成功")    

# 主函数
def main():
    # 创建MySQL连接
    connection2 = create_mysql_connection("localhost", "root", "123456", "world")
    cursor2 = connection2.cursor()
    
    del_user_retain_function(connection2,cursor2)

    # 关闭数据库连接
    connection2.close()

if __name__ == "__main__":
    main()            
import requests
from datetime import datetime, timedelta
import calendar


def get_json_data_with_cookie(url,cookie_value):
    try:
        headers = {
            'cookie': f'PHPSESSID={cookie_value}',  # 替换为实际的Cookie名和值
        }
        response = requests.get(url,headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        json_data = response.json()
        return json_data
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

def caculate_month(m):
    if m == 12 :
        return 1
    else :
        return m+1
def calulate_year(y,m):
    if m == 12 :
        return y+1
    else :
        return y

def get_sort_data(type):
        cookie_value = "4c266264513f1e75fad4ccb9c5e4401f"
        year = 2023
        month = 8

        next_year = calulate_year(year,month)
        next_month = caculate_month(month)

        # 获取这个月的第一天和最后一天
        first_day = f"{year}-{month:02d}-01"
        last_day = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"


        # 获取月的第一天和最后一天
        next_first_day = f"{next_year}-{next_month:02d}-01"
        next_last_day = f"{next_year}-{next_month:02d}-{calendar.monthrange(next_year, next_month)[1]}"

        zhucedate = f"{first_day}+-+{last_day}"
        if type == 1 :
            huoyuedate = ""
            is_verify = ""
            api_url = f"https://admin.mingpinjixuan.com/admin/user.User/get_user_list.html?page=1&limit=20&nickname=&is_verify={is_verify}&pid=&register_time={zhucedate}&verify_time=&user_time_type=visit&user_time={huoyuedate}"  # 替换为实际的接口URL

            json_data = get_json_data_with_cookie(api_url, cookie_value)
    
            if json_data and "data" in json_data:
                data_array = json_data["data"]
                menber_count = json_data["count"]
                array_length = len(data_array)
                print(f"{year}-{month}总数为:", menber_count)
                print("\n")
 
            else:
                print("No 'data' field found in the JSON response.")
        elif type == 2 :
            huoyuedate = ""
            is_verify = "1"
            api_url = f"https://admin.mingpinjixuan.com/admin/user.User/get_user_list.html?page=1&limit=20&nickname=&is_verify={is_verify}&pid=&register_time={zhucedate}&verify_time=&user_time_type=visit&user_time={huoyuedate}"  # 替换为实际的接口URL

            json_data = get_json_data_with_cookie(api_url, cookie_value)
    
            if json_data and "data" in json_data:
                data_array = json_data["data"]
                menber_count = json_data["count"]
                array_length = len(data_array)
                print(f"{year}-{month}实名总数为:", menber_count)
                print("\n")
 
            else:
                print("No 'data' field found in the JSON response.")
        elif type == 3 :
            huoyuedate = f"{next_first_day}+-+{next_last_day}"
            is_verify = ""
            api_url = f"https://admin.mingpinjixuan.com/admin/user.User/get_user_list.html?page=1&limit=20&nickname=&is_verify={is_verify}&pid=&register_time={zhucedate}&verify_time=&user_time_type=visit&user_time={huoyuedate}"  # 替换为实际的接口URL

            json_data = get_json_data_with_cookie(api_url, cookie_value)
    
            if json_data and "data" in json_data:
                data_array = json_data["data"]
                menber_count = json_data["count"]
                array_length = len(data_array)
                print(f"{year}-{month}次月活跃总数为:", menber_count)
                print("\n")
 
            else:
                print("No 'data' field found in the JSON response.")
        elif type == 4 :
            huoyuedate = f"{next_first_day}+-+{next_last_day}"
            is_verify = "1"
            api_url = f"https://admin.mingpinjixuan.com/admin/user.User/get_user_list.html?page=1&limit=20&nickname=&is_verify={is_verify}&pid=&register_time={zhucedate}&verify_time=&user_time_type=visit&user_time={huoyuedate}"  # 替换为实际的接口URL

            json_data = get_json_data_with_cookie(api_url, cookie_value)
    
            if json_data and "data" in json_data:
                data_array = json_data["data"]
                menber_count = json_data["count"]
                array_length = len(data_array)
                print(f"{year}-{month}次月实名活跃总数为:", menber_count)
                print("\n")
 
            else:
                print("No 'data' field found in the JSON response.")
def main():

    loops = [1,2,3,4]

    for i in loops : 
        get_sort_data(i)

if __name__ == "__main__":
    main()


import requests
from bs4 import BeautifulSoup

# 定义要获取按钮的目标网站的URL
url = "https://www.onwsw.com/company/tianliaolei/"  # 将实际的网站URL替换为目标网站

# 发起HTTP请求获取网页内容
response = requests.get(url)
html_content = response.content

# 使用BeautifulSoup解析HTML内容
soup = BeautifulSoup(html_content, 'html.parser')

# 查找特定的<div>元素，
#target_divs = soup.find_all(class_='seeProduct')

#
class_pages = soup.find('div',class_='pages')

if class_pages :
    pages_count = class_pages.findAll('a')

    print(len(pages_count))
    # 打印找到的<a>标签的文本和链接
    for link in pages_count:
        link_text = link.text.strip()
        link_href = link.get('href')
        print("Link Text:", link_text)
        print("Link Href:", link_href)
else :
    print('not found')


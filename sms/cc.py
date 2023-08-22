from bs4 import BeautifulSoup
import requests

def get_sort_name(url,type,name) :
    response = requests.get(url)#opener.open(url)
    soup = BeautifulSoup(response.content, "html.parser")
    soup.find_all

#获取网页上指定div元素下的a标签链接
def get_url_links(url,class_name):

    response = requests.get(url)#opener.open(url)
    #print(url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    for parent in soup.find_all("div", {"class": class_name}):
        #param = parent.find_all("a", href=True)
        #param['first_sort'] = parent.find('p',{'class':'floor_c_title'}).text.strip()  
        links.extend(parent.find_all("a", href=True))
        
    return links

#获取网页上指定元素下的a标签链接，可指定查找第一个还是全部
def get_url_links_with_param(url,class_name,find_class,isAll):

    response = requests.get(url)#opener.open(url)
    #print(url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    #查找所有
    if isAll == True :
        for parent in soup.find_all(find_class, {"class": class_name}):  
            links.extend(parent.find_all("a", href=True))

    else :
        link = soup.find(find_class,class_name)
        links.extend(link.find_all("a", href=True))        
    return links

#获取分类分页数据    
def get_company_all_page_data(url,class_name):
    response = requests.get(url)#opener.open(url)
    soup = BeautifulSoup(response.content, "html.parser")
    #存在分页数据
    if soup.find('div',{'class':class_name}):

        a_links = soup.find('div',{'class':class_name}).find_all('a')
        company_urls = []
        #循环遍历分页数据
        for company_url in get_page_links(url,a_links) :
            #解析分页数据，获得公司url
            company_infos = parse_company_info(get_soup(company_url))
            company_urls.extend(company_infos)
        return company_urls
    else :
        #无分页数据，直接解析获取公司url
        return parse_company_info(soup)

#获取公司信息网页html
def get_soup(url) :
    response =  requests.get(url)#opener.open(url)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup


#解析公司列表信息数据
def parse_company_info(soup):
    #查找a标签下的url
    div_content = soup.find_all('a',{'class':'seeProduct'}) 
    #循环插入url至数组
    company_urls = [link['href'] for link in div_content]
    
    return company_urls
        

#获取分页的url地址数组
def get_page_links(url,links) :

    urls = [url] + [f'{url}{i}.html' for i in range(2,len(links)+1)]
    return urls        


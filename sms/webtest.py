
import cc

def get_company_urls(url) :
    #获取首页分类下的url地址
    linkss = cc.get_url_links(url,'floor_c_con')
    all_company_urls = []
    count = 0
    for main_link in linkss :
        if main_link.get('class') == 'floor_c_title' :
            print(main_link)
        else :
            #获取所有二级分类下的url
            company_sorts = cc.get_url_links_with_param(main_link['href'],'showCon','dd',False)  
        
            for sort_link in company_sorts :
            
                #获取二级分类下的公司url
                all_page_data = cc.get_company_all_page_data(sort_link['href'],'pages')
                all_company_urls.extend(all_page_data)
   
    return all_company_urls

def main() :
    url = 'https://www.onwsw.com/company/'
    get_company_urls(url)
  

if __name__ == '__main__' :

    #print(cc.get_company_all_page_data('https://www.onwsw.com/company/chengmowuzhi/','pages'))
    main()
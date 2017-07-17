# -*- coding:utf-8 -*-

from bs4 import BeautifulSoup
from sqlserver import SqlServer
from mylog import MyLog
import re
import sys
import requests
import threading
# 短时间内从此网站获取大量信息会被403禁止访问，所以需要配合selenium来获取源码
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class SpiderConfig(object):
    # LOG_NAME = sys.argv[0][0:-3] + '.log'

    session = requests.session()
    db = SqlServer(host='www.example.com', user='username', pwd='password', db='databaseName')

    def __init__(self):
        self.PC_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                           'Accept-Encoding': 'gzip, deflate',
                           'Accept-Language': 'zh-CN,zh;q=0.8',
                           'Connection': 'keep-alive',
                           'Referer': 'www.baidu.com',
                           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'}

    def get_urls(self, start_url, re_pattern):
        """
        获取起始页面中的目标文章url
        :param start_url: 传入起始url
        :param re_pattern: 传入正则表达式来筛选出目标文章打的url
        :return: 目标文章的所有url
        """
        list_url = []
        try:
            SpiderConfig.session.headers = self.PC_HEADERS
            response = SpiderConfig.session.get(start_url, timeout=30)
            if response.status_code == 200:
                html = response.content
            else:
                # 短时间内从此网站获取大量信息会被403禁止访问，所以需要配合selenium来获取源码
                chrome = webdriver.Chrome()
                chrome.get(start_url)
                assert "网贷天眼" in chrome.title
                html = chrome.page_source
                chrome.quit()
            soup = BeautifulSoup(html, 'lxml')
            urls = soup.find('div', class_="mod-listbox active").findAll('a', href=re.compile(re_pattern))
            for url in urls:
                if url['href'] not in list_url:
                    list_url.append(url['href'])
                else:
                    print "发现重复链接:" + str(url['href']) + ' 已清除'
        except requests.ConnectTimeout:
            print "url请求超时"

        if len(list_url) >= 1:
            return list_url
        else:
            print "目标url列表数量较少，请检查代码。"

    def get_htmls(self, urls):
        """
        从获取到的目标url列表里获取各自页面的html源码
        :param urls: 传入获取到的url列表
        :return: 目标html列表
        """
        list_html = []
        for url in urls:
            try:
                SpiderConfig.session.headers = self.PC_HEADERS
                response = SpiderConfig.session.get(url, timeout=30)
                if response.status_code == 200:
                    html = response.content
                else:
                    # 短时间内从此网站获取大量信息会被403禁止访问，所以需要配合selenium来获取源码
                    # driver = webdriver.Chrome()
                    # driver.get(url)
                    # assert "网贷新闻" in driver.title
                    # html = driver.page_source
                    # #print html
                    # driver.quit()
                    # 使用phantomjs的请求头部抓取此网站会被封锁
                    dcap = dict(DesiredCapabilities.PHANTOMJS)
                    dcap['phantomjs.page.settings.userAgent'] = (
                        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
                    )
                    # 禁止下载图片加快加载速度service_args=['--load-images=false']
                    driver = webdriver.PhantomJS(desired_capabilities=dcap, service_args=['--load-images=false'])
                    driver.get(url)
                    assert "网贷新闻" in driver.title
                    html = driver.page_source
                    driver.quit()
                if html:
                    list_html.append(BeautifulSoup(html, 'lxml'))
                else:
                    raise ValueError
            except requests.ConnectTimeout:
                print "url请求超时"
        if list_html:
            return list_html
        else:
            print "目标html列表为空，请检查代码。"

    def clean_str(self, old_str):
        """
        由里到外第一遍清除外链，第二遍清除图片，第三遍清除<span>标签，第四遍清除<a>标签, 第五遍清楚class属性
        第六遍清除</a>，第七遍清除</span>,第八遍清除标签里的id
        :param old_data:传入要清洗的字符串
        :return: 清洗后的字符串
        """
        new_str = re.sub(re.compile(' *id="(.*?)"'), '',
                         re.sub(re.compile('</span>'), '',
                                re.sub(re.compile('</a>'), '',
                                       re.sub(re.compile(' *class="(.*?)"'), '',
                                              re.sub(re.compile('<a(.*?)>'), '',
                                                     re.sub(re.compile('<span(.*?)>'), '',
                                                            re.sub(re.compile('<img alt=(.*?)/>'), '',
                                                                   re.sub(re.compile(
                                                                       'a href="http:(.*?)" target="_blank">'), '', old_str))))))))
        new_str = re.sub(re.compile('责任编辑：gold'), '责任编辑：小金', new_str)
        return new_str

    def check_newest_data(self, select_sql, list_headline):
        """
        检查数据库最新的文章是否与数据源最新的数据一样
        :param select_sql: 传入获取数据库最新的文章标题的sql语句
        :param list_headline: 传入已获取到的所有文章列表
        :return: 返回更新数字，是最新则返回0，不是最新则返回要更新的次数
        """
        db = SpiderConfig.db
        # 检查数据库最新数据与最新数据源是否一样,不是则返回True
        newest_headline = ""  # 数据源的最新标题
        # select_sql = "select headline from [zy_analyiss] WHERE aid=(select MAX(aid) from [zy_analyiss] WHERE zy_type='%s')" %data_name
        # 从数据库获取最新的文章标题
        if db.ExecQuery(select_sql):
            newest_headline = db.ExecQuery(select_sql)[0][0]
            # for trupe_ in db.ExecQuery(select_sql):
            #     for str_ in trupe_:
            #         newest_headline = str_
        else:
            newest_headline = "null"

        # 检查数据源的最新标题是否和数据库最新文章标题一致，是则返回True
        def check_update(headline):
            if newest_headline != "null":
                if newest_headline != headline:
                    return True
                else:
                    return False
            else:
                return True

        # 如果数据库最新数据与数据源最新数据不一样，则加一次要更新的数字
        update_num = 0
        for i in range(len(list_headline)):
            if check_update(list_headline[i]):
                update_num += 1
            else:
                break

        return update_num


class P2PNews(SpiderConfig):
    log = MyLog()

    def __init__(self):
        SpiderConfig.__init__(self)
        self.START_URL = "http://news.p2peye.com/wdxw/"
        self.url_re_pattern = "http://news\.p2peye\.com/article\-[0-9]+\-1\.html"

    @log.deco_log(sys.argv[0][0:-3] + '.log', "get_data", False)
    def get_data(self):
        list_url = SpiderConfig.get_urls(self, self.START_URL, self.url_re_pattern)
        list_html = SpiderConfig.get_htmls(self, list_url)
        list_headline = []
        list_date = []
        list_content = []
        check_data = lambda html: html.get_text() if html else "null"
        for soup in list_html:
            headline = soup.find('h1')
            list_headline.append(check_data(headline))
            #print check_data(headline)
            date = re.findall('[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}', str(soup), re.S)
            list_date.append(date[0])
            print date[0]
            content = soup.find('div', class_="d donoe")
            if content:
                content = SpiderConfig.clean_str(self, str(content))
            else:
                raise ValueError
            # print content
            list_content.append(content)

        return list_headline, list_date, list_content

    @log.deco_log(sys.argv[0][0:-3] + '.log', "update_data", False)
    def update_data(self, *tuple_data):
        list_headline, list_date, list_content = tuple_data[0][0], tuple_data[0][1], tuple_data[0][2]
        db = SpiderConfig.db
        select_sql = "select heading from [zy_news] WHERE nid=(select MAX(nid) from [zy_news])"
        update_num = SpiderConfig.check_newest_data(self, select_sql, list_headline)
        if update_num != 0:
            for i in range(update_num)[::-1]:
                insert_sql = "INSERT INTO [zy_news] VALUES ('{}','{}','{}')".format(
                        list_headline[i], list_date[i], list_content[i]
                )
                db.ExecNonQuery(insert_sql.encode('utf-8'))
                print list_headline[i]
                print list_date[i]
                print list_content[i]
            print "%s页面数据上传更新完毕" % "P2P新闻"
        else:
            print "%s页面数据源无最新数据更新" % "P2P新闻"

num = 1


def run_timer():
    global num
    print "------------------正在启动第%s次数据更新------------------" % str(num)
    num += 1
    try:
        p = P2PNews()
        p.update_data(p.get_data())
        timer = threading.Timer(7200, run_timer)
        timer.start()
    except Exception, e:
        print "报错原因：" + str(e)
        run_timer()

if __name__ == '__main__':
    print "已启动"
    timer = threading.Timer(0, run_timer)
    timer.start()

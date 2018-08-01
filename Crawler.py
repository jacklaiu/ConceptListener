from bs4 import BeautifulSoup
from selenium import webdriver
import os
import time

def getSoupsFromWencai(w):
    url = 'https://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=3&qs=pc_~soniu~stock~stock~history~query&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=' + w
    browser = None
    ret = []
    try:
        browser = webdriver.Chrome(
            os.path.abspath(os.path.dirname(__file__)) + '\driver\chromedriver.exe')
        browser.get(url)
        browser.implicitly_wait(1)
        time.sleep(2)
        try:
            isClicked = False
            elem70 = browser.find_element_by_css_selector('#table_foot_bar select option[value="70"]')
            if elem70 is not None and isClicked == False:
                elem70.click()
                time.sleep(4)
        except Exception as e:
            print("")

        html = browser.execute_script("return document.documentElement.outerHTML")
        soup = BeautifulSoup(html, "html.parser")
        ret.append(soup)

        next = browser.find_element_by_css_selector('#pageBar #next')
        nowSoup = soup
        while True:
            next = browser.find_element_by_css_selector('#pageBar #next')
            disable_next_str_arr = nowSoup.select('#pageBar #next')[0].get('class')
            disable_next_str = ''.join(disable_next_str_arr)
            if 'disable' not in disable_next_str:
                try:
                    next.click()
                except:
                    return ret
                browser.implicitly_wait(1)
                time.sleep(2)
                html = browser.execute_script("return document.documentElement.outerHTML")
                soup = BeautifulSoup(html, "html.parser")
                ret.append(soup)
                nowSoup = soup
            else:
                return ret
    except Exception as e:
        return None
    finally:
        browser.close()
    return ret
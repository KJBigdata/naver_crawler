import requests
from bs4 import BeautifulSoup
import re
import datetime
import time
import pickle
import glob
import numpy as np
from multiprocessing import Pool
from fake_useragent import UserAgent
from collections import Counter
import os
import argparse

class Crawler:
    def __init__(self, start_date = '20190801', end_date='20200731'):
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = 'https://news.naver.com/main/list.nhn?mode=LPOD&mid=sec&listType=paper'
        self.press_code = [{'014': '파이낸셜뉴스'}, {'018': '이데일리'}, {'277': '아시아경제'}, {'469': '한국일보'},
                           {'021': '문화일보'}, {'023': '조선일보'}, {'028': '한겨레'},
                           {'015': '한국경제'}, {'011': '서울경제'},
                           {'016': '헤럴드경제'}, {'029': '디지털타임스'}]

    def get_all_date(self):
        d1 = datetime.datetime.strptime(self.start_date, '%Y%m%d').date()
        d2 = datetime.datetime.strptime(self.end_date, '%Y%m%d').date()
        date_list = [str(d1 + datetime.timedelta(days=x)).replace('-','') for x in range((d2 - d1).days + 1)]
        return date_list

    def get_url_list(self, main_url):
        main_url = requests.get(main_url)
        soup = BeautifulSoup(main_url.text, "html.parser")
        title_url_pair_list = [(a.text.strip(), str(a).split('href="')[1].split('"')[0].replace('amp;','')) if a.find('img') is None
                               else (a.find('img').get('alt').strip(), str(a).split('href="')[1].split('"')[0].replace('amp;',''))
                               for a in soup.select('dt > a')]
        return title_url_pair_list

    def remove_duplicate(self, title_url_pair_list = list):
        my_set = set()
        res = []
        for title, url in title_url_pair_list:
            if url not in my_set:
                res.append((title.strip().replace("\xa0", ' ').replace("\'",''),url))

                my_set.add(url)
        print(res)
        return res

    def sub_summary(self, text, summary=list):
        text = text.replace("\xa0", ' ')
        sub_word = []
        for i, phrase in enumerate(summary):
            sub_word.append(text[:text.find(summary[i]) + len(summary[i])])
            text = text[text.find(summary[i]) + len(summary[i]):]
        print('substring_summary from full_text : ', sub_word)
        return text.strip()

    def remove_naver_pattern(self, text):
        if text.find('이 기사는 언론사에서') == '-1':
            return text
        else:
            text = text[:text.find('이 기사는 언론사에서')].strip()
            return text

    def parse_url(self, title_url_pair=tuple):
        print(title_url_pair)
        title = title_url_pair[0]
        url = title_url_pair[1]
        ua = UserAgent()
        headers = {'User-Agent': ua.random}
        article = requests.get(url, headers=headers)
        soup = BeautifulSoup(article.content.decode('euc-kr', 'replace'), "html.parser")
        try:
            if soup.find('strong', attrs={"class" : "media_end_summary"}) is not None:
                summary = soup.find('strong', attrs={"class": "media_end_summary"})
                br_num = len([n for n in [br for br in summary] if str(n) == '<br/>'])
                if br_num > 1 :
                    summary = [str(s).replace("\xa0", ' ') if str(type(s)) == "<class 'bs4.element.NavigableString'>"
                               else s.text for s in [n for n in [br for br in summary] if str(n) != '<br/>']]
                    category = soup.find('em', attrs={'class': 'guide_categorization_item'}).text
                    content = soup.find("div", attrs={"id": "articleBody"}).text
                    content = self.sub_summary(content, summary)
                    content = self.remove_naver_pattern(content)

                    doc = {'url': url, 'category' : category, 'title': title, 'summary' : summary, 'content': content}
                    keys = ['category', 'title', 'summary', 'url']
                    print('***', [doc.get(key) for key in keys], '***')
                    return doc
                else:
                    print('summary 길이가 한줄 이하입니다.')
            elif soup.find('b') is not None:
                summary = soup.find('b')
                br_num = len([str(n).replace("\xa0", ' ') for n in [br for br in summary] if str(n) == '<br/>'])
                if br_num > 1 :
                    summary = [str(s).replace("\xa0", ' ') if str(type(s)) == "<class 'bs4.element.NavigableString'>"
                               else s.text for s in [n for n in [br for br in summary] if str(n) != '<br/>']]
                    category = soup.find('em', attrs={'class': 'guide_categorization_item'}).text
                    content = soup.find("div", attrs={"id": "articleBody"}).text
                    content = self.sub_summary(content, summary)
                    content = self.remove_naver_pattern(content)

                    doc = {'url': url, 'category' : category, 'title': title, 'summary': summary, 'content': content}
                    keys = ['category', 'title', 'summary', 'url']
                    print('***', [doc.get(key) for key in keys], '***')
                    return doc
                else :
                    print('summary 길이가 한줄 이하입니다.')
                    pass
            else:
                print('summary가 존재하지 않습니다.')
                pass

        except Exception as ex:
            print('None articleBody id, you can check it on {} '.format(url))
            pass

    def run(self):
        date_list = self.get_all_date()
        for info in self.press_code:
            whole_docs = []
            count = []
            code = list(info.keys())[0]
            press = list(info.values())[0]

            for date in date_list:
                for page in range(1,4):

                    start_time = time.time()
                    main_url = self.base_url +'&oid={}'.format(code) + '&date={}'.format(date) + '&page={}'.format(page)
                    title_url_pair_list = self.remove_duplicate(self.get_url_list(main_url))
                    print('-------Crawling {} news {}page on {}, 총 {}건-------'.format(press, page, date, len(title_url_pair_list)))
                    if title_url_pair_list == []:
                        print('summary 조건에 충족된 기사가 존재하지 않습니다.')
                        pass
                    else:
                        try:
                            pool = Pool(processes=20)
                            docs = pool.map(func=self.parse_url, iterable=title_url_pair_list)
                            docs = [doc for doc in docs if doc is not None if len(doc['summary']) > 1]
                            info['press'] = press
                            info['date'] = date
                            for doc in docs:
                                doc.update(info)
                                del doc[code]

                            whole_docs.extend(docs)
                            count.append(len(docs))
                            print("---------------{}page 내 {}건 삽입------------------".format(page, len(docs)))
                            print("---------------지금까지 최종 {}건 삽입------------------".format(sum(count)))
                            pool.close()
                            # pool.join()
                            time.sleep(5)
                        except requests.ConnectionError as e:
                            print(str(e))
                            print("Connection refused by the server..")
                            print("Let me sleep for 30 seconds")
                            time.sleep(30)
                            continue
                        except Exception as ex:
                            print(str(ex))
                            time.sleep(30)
                            continue

                    print("----------------%s seconds ----------------" % round(float(time.time() - start_time), 2))
                    print('\n')

            with open('./' + press + '.pkl', 'wb') as f1:
                print("---------------최종 {}건 삽입------------------".format(len(whole_docs)))
                pickle.dump(whole_docs, f1)
                time.sleep(30)

    def show_stat(self):
        files = np.array(glob.glob("./*.pkl"))

        category = {}
        size = 0
        for i, f in enumerate(files):
            file = f
            with open(f, 'rb') as f:
                whole_doc = pickle.load(f)
                press = f.name[2:].split('.')[0]
                print(press, '기사 갯수', len(whole_doc), np.round(os.stat(file).st_size / 1000000, 2), 'MB')
                size += os.stat(file).st_size / 1000000

                category_count = Counter([doc['category'] for doc in whole_doc])
                summary_count = Counter([len(doc['summary']) for doc in whole_doc])

                print("*summary_count : ", summary_count)
                print("*category_count : ", category_count)
                print('\n')
                for key, value in category_count.items():
                    if key in list(category.keys()):
                        category[key] += value
                    else:
                        category[key] = value
        print("***All count of naver news by category: ", category)
        print('총 합계 : ', sum(category.values()), '개')
        print('총 크기 : ', size, 'MB')

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date

    engine = Crawler(start_date = START_DATE, end_date=END_DATE)
    engine.run()
    engine.show_stat()


if __name__ == '__main__':
    main()
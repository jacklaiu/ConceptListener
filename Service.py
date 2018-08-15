
# -*- coding: gbk -*-
from flask import Flask, abort, request, jsonify
import Dao as dao
import Fortest as ft
import tushare as ts
app = Flask(__name__, static_url_path='')
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def hello_world():
    return 'Hello World!'

# @app.route('/get_the_codes/<datestr>/<timestr>')
# def get_the_codes(datestr=None, timestr=None):
#     concept_codes_rel = {}
#     code_item_rel = {}
#     code_basics_rel = {}
#     concept_chg_rel = {}
#     code_concepts_rel = {}
#     concept_zhangtingcount_rel = {}
#     code_price_rel = None
#     code_pre_close_rel = None
#     code_open_rel = None
#     if datestr is not None and timestr is not None:
#         map = ft.get_code_price(datestr, timestr)
#         code_price_rel = map['code_price']
#         code_pre_close_rel = map['code_pre_close']
#         code_open_rel = map['code_open']
#     df_basics = ts.get_stock_basics()
#     for row in df_basics.iterrows():
#         code = row[0]
#         code_basics_rel.setdefault(code, row[1]) if code not in code_basics_rel.keys() else code_basics_rel[code].append(row[1])
#
#     codes = [item['code'] for item in dao.select("SELECT DISTINCT code from t_security_concept", ())]
#
#     ##concept_codes_rel################################
#     arr = dao.select("select code, concept from t_security_concept", ())
#     for item in arr:
#         code = item['code']
#         concept = item['concept']
#         concept_codes_rel.setdefault(concept, [code]) if concept not in concept_codes_rel.keys() else concept_codes_rel[concept].append(code)
#         code_concepts_rel.setdefault(code, [concept]) if code not in code_concepts_rel.keys() else code_concepts_rel[code].append(concept)
#
#     ##code_item_rel################################
#     count = 0
#     while count < codes.__len__():
#         dfs = ts.get_realtime_quotes(codes[count:count+20])
#         for row in dfs.iterrows():
#             code = row[1]['code']
#             code_item_rel.setdefault(code, row[1])
#         print(codes[count:count+20])
#         count = count + 20
#
#     ##concept_chg_rel: 当前时刻，板块涨跌幅>2//或者<1//或者开盘板块涨跌幅>0.5################################################
#     for concept in concept_codes_rel.keys():
#         codes = concept_codes_rel[concept]
#         if codes.__len__() < 4:
#             continue
#         pre = 0
#         now = 0
#         begin = 0
#         count = 0
#         zhangting_count = 0
#         for code in codes:
#             item = code_item_rel[code]
#             open = float(item['open'])
#             pre_close = float(item['pre_close'])
#             price = float(item['price'])
#             rate = round((price-pre_close)/pre_close*100,2)
#             if rate > 9.88:
#                 zhangting_count = zhangting_count + 1
#             if code_price_rel is not None:
#                 if code not in code_open_rel.keys() or code not in code_pre_close_rel.keys() or code not in code_price_rel.keys():
#                     continue
#                 open = float(code_open_rel[code])
#                 pre_close = float(code_pre_close_rel[code])
#                 price = float(code_price_rel[code])
#
#             if price == 0.0:
#                 continue
#             basic = code_basics_rel[code]
#             outstanding = float(basic['outstanding']*100000000)
#             pre_liquid_assets = outstanding * pre_close
#             liquid_assets = outstanding * price
#             begin_assets = outstanding * open
#             pre = pre + pre_liquid_assets
#             now = now + liquid_assets
#             begin = begin + begin_assets
#             count = count + 1
#         rate = round((now-pre)/pre*100, 2)
#         open_rate = round((begin-pre)/pre*100, 2)
#         concept_zhangtingcount_rel.setdefault(concept, zhangting_count)
#         if zhangting_count == 0:
#             continue
#         if open_rate > 0.5 or open_rate < -0.2:
#             continue
#         if rate < 1 or rate > 2:
#             continue
#         # if rate < 2:
#         #     continue
#         concept_chg_rel.setdefault(concept, rate)
#
#     ##完成概念板块chg计算，下面做获取符合条件的股票做准备
#     ret_codes = []
#     for concept in concept_chg_rel.keys():
#         codes = concept_codes_rel[concept]
#         for code in codes:
#             ret_codes.append(code)
#
#     h_zhangting_count = 0
#     zhangting_count = 0
#     for code in ret_codes:
#         item = code_item_rel[code]
#         high = float(item['high'])
#         pre_close = float(item['pre_close'])
#         price = float(item['price'])
#         rate = round((price - pre_close) / pre_close * 100, 2)
#         h_rate = round((high - pre_close) / pre_close * 100, 2)
#
#         if h_rate > 9.89 and rate > 9.89:
#             zhangting_count = zhangting_count + 1
#         if h_rate > 9.89:
#             h_zhangting_count = h_zhangting_count + 1
#     if h_zhangting_count == 0:
#         print("success: " + str(0))
#     else:
#         print("success: " + str(zhangting_count/h_zhangting_count))
#
#     #     for code in codes:
#     #         if code not in code_count_rel.keys():
#     #             code_count_rel.setdefault(code, 1)
#     #         else:
#     #             code_count_rel[code] = code_count_rel[code] + 1
#     #
#     # ret_codes = []
#     # ##获取相关的有两个概念符合条件的code，获取到的code，当前时刻涨跌幅小于2
#     # for code in code_count_rel.keys():
#     #     count = code_count_rel[code]
#     #     if count > 1:
#     #         item = code_item_rel[code]
#     #         pre_close = float(item['pre_close'])
#     #         price = float(item['price'])
#     #         rate = round((price - pre_close)/pre_close * 100, 2)
#     #         if rate > 2:
#     #             continue
#     #         ret_codes.append(code)
#     #
#     # for code in ret_codes:
#     #     concepts = code_concepts_rel[code]
#     #     print("Code: " + code + " Concepts: " + str(concepts))
#     #
#     # temp_codes = []
#     # code_liquidAssets_rel = {}
#     # for code in ret_codes:
#     #     if float(code_basics_rel[code]['npr']) < 0:
#     #         continue
#     #     liquidAssets = float(code_basics_rel[code]['liquidAssets'])
#     #     code_liquidAssets_rel.setdefault(code, liquidAssets)
#     #
#     # list = sorted(code_liquidAssets_rel.items(), key=lambda d: d[1])
#     # for item in list:
#     #     code = item[0]
#     #     liquidAssets = code_liquidAssets_rel[code]
#     #     temp_codes.append(code)
#     #     print("Code: " + code + " liquidAssets: " + str(liquidAssets))
#     #
#     # ret_codes = temp_codes
#
#     return jsonify(ret_codes)

@app.route('/get_realtime_concept_chg')
def get_realtime_concept_chg():
    concept_codes_rel = {}
    code_item_rel = {}
    code_basics_rel = {}
    concept_chg_rel = {}
    df_basics = ts.get_stock_basics()
    for row in df_basics.iterrows():
        code = row[0]
        code_basics_rel.setdefault(code, row[1]) if code not in code_basics_rel.keys() else code_basics_rel[code].append(row[1])

    codes = [item['code'] for item in dao.select("SELECT DISTINCT code from t_security_concept", ())]
    ##concept_codes_rel################################
    arr = dao.select("select code, concept from t_security_concept", ())
    for item in arr:
        code = item['code']
        concept = item['concept']
        concept_codes_rel.setdefault(concept, [code]) if concept not in concept_codes_rel.keys() else concept_codes_rel[concept].append(code)

    print(concept_codes_rel.keys().__len__())
    ##code_item_rel################################
    count = 0
    while count < codes.__len__():
        dfs = ts.get_realtime_quotes(codes[count:count+20])
        for row in dfs.iterrows():
            code = row[1]['code']
            code_item_rel.setdefault(code, row[1])
        print(codes[count:count+20])
        count = count + 20

    ##concept_chg_rel################################################
    for concept in concept_codes_rel.keys():
        codes = concept_codes_rel[concept]
        pre = 0
        now = 0
        count = 0
        for code in codes:
            item = code_item_rel[code]
            pre_close = float(item['pre_close'])
            price = float(item['price'])
            if price == 0.0:
                price = pre_close
            basic = code_basics_rel[code]
            outstanding = float(basic['outstanding']*100000000)
            pre_liquid_assets = outstanding * pre_close
            liquid_assets = outstanding * price
            pre = pre + pre_liquid_assets
            now = now + liquid_assets
            count = count + 1
        if count < 4: continue
        rate = round((now-pre)/pre*100, 2)
        concept_chg_rel.setdefault(concept, rate)
    list = sorted(concept_chg_rel.items(), key=lambda d: d[1], reverse=True)
    return jsonify(list)

if __name__ == '__main__':
    app.run()











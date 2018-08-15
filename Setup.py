import Crawler as cr
import Dao as dao
import Util as ut
import tushare as ts

# 刷新数据库内股票对应的板块+概念
def refresh_BaseConceptAndIndustryData():
    print("refresh_BaseConceptAndIndustryData: 在网络上获取信息，保存证券、概念、行业关系信息")
    dao.update("truncate table t_security_concept", ())
    dao.update("truncate table t_security_industry", ())
    soups = cr.getSoupsFromWencai("a股；所属概念；所属同花顺行业；")
    code_concepts = {}
    code_industrys = {}
    industry_level = {}
    code_name = {}
    for soup in soups:
        es_code = soup.select('#resultWrap .static_con_outer .tbody_table tr td.item div.em')
        index = 0
        codes = []
        while index < es_code.__len__():
            o_str = es_code[index].text.strip()
            if (o_str.isdigit()):
                codes.append(o_str)
                code = o_str
                code_concepts.setdefault(o_str, [])
                index = index + 1
                o_str = es_code[index].text.strip()
                code_name.setdefault(code, o_str)
            index = index + 1
        # 概念获取
        es_c0 = soup.select('#tableWrap .scroll_tbody_con .scroll_table tr')
        count = 0
        for el in es_c0:
            a_els = el.select('td[colnum="5"] div span.fl a')
            code = codes[count]
            for a_el in a_els:
                concept = a_el.text.strip()
                code_concepts[code].append(concept)
                print('Code: ' + code + '->Concept: ' + concept)
            count = count + 1
        # 行业获取
        es_c1 = soup.select('#tableWrap .scroll_tbody_con .scroll_table tr')
        count = 0
        for el in es_c1:
            a_els = el.select('td[colnum="6"] div span.fl a')
            code = codes[count]
            level = 1
            for a_el in a_els:
                industry = a_el.text.strip()

                if code not in code_industrys.keys():
                    code_industrys.setdefault(code, [industry])
                else:
                    code_industrys[code].append(industry)
                if industry not in industry_level.keys():
                    industry_level.setdefault(industry, level)

                print('Code: ' + code + '->industry: ' + industry + '->Level: ' + str(level))
                level = level + 1
            count = count + 1

    values = []
    for code in code_concepts.keys():
        concepts = code_concepts[code]
        _values = [(code, code_name[code], concept) for concept in concepts]
        values = values + _values
        # for concept in concepts:
        #     name = code_name[code]
        #     values.append((code, name, concept))
    dao.updatemany('insert into t_security_concept(code, name, concept) values(%s,%s,%s)', values)

    values = []
    for code in code_industrys.keys():
        industrys = code_industrys[code]
        _values = [(code, code_name[code], industry, industry_level[industry]) for industry in industrys]
        values = values + _values
        # for industry in industrys:
        #     name = code_name[code]
        #     level = industry_level[industry]
        #     values.append((code, name, industry, level))

    dao.updatemany('insert into t_security_industry(code, name, industry, level) values(%s,%s,%s,%s)', values)

def refresh_BaseConceptRelationshipData():
    print("refresh_BaseConceptRelationshipData: 概念间关联强度")
    dao.update("truncate table t_security_concept_relationship", ())
    concepts = [item['concept'] for item in dao.select("select distinct concept from t_security_concept", ())]
    values = []
    concept_count_rel = {}
    for concept in concepts:
        print("处理概念: " + concept)
        concepts_rel = [item['concept'] for item in dao.select("select concept from t_security_concept where code in (select code from t_security_concept where concept = %s)", (concept))]
        for concept_rel in concepts_rel:
            if concept_rel not in concept_count_rel.keys():
                concept_count_rel.setdefault(concept_rel, 1)
            else:
                concept_count_rel[concept_rel] = concept_count_rel[concept_rel] + 1
        for concept_rel in concept_count_rel:
            values.append((concept, concept_rel, concept_count_rel[concept_rel]))
    dao.updatemany("insert into t_security_concept_relationship(concept, concept_rel, count) values(%s,%s,%s)", values)


def refreshSecurityDaily(end=ut.getLastestOpenDate()):
    dao.update("truncate table t_security_daily", ())
    start = ut.preOpenDate(end, 100)
    securitys = [item['code'] for item in dao.select("select distinct code from t_security_concept", ())]
    values = []
    for code in securitys:
        df = ts.get_k_data(code, start, end)
        pre_close = None
        for items in df.iterrows():
            row = items[1]
            if pre_close is None:
                pre_close = row['close']
                continue
            date = row['date']
            code = row['code']
            open = row['open']
            close = row['close']
            high = row['high']
            low = row['low']
            print("Values Len:" + str(values.__len__()) + " Code: " + code + " Date: " + str(date))
            values.append((code, pre_close, high, close, low, open, date))
            pre_close = close
            if values.__len__() == 25000:
                print("saving 2 db ing...")
                dao.updatemany("insert into t_security_daily(code, pre_close, high, close, low, open, date) values(%s,%s,%s,%s,%s,%s,%s)", values)
                values = []

def refreshSecurityDisposition():
    dao.update("truncate table t_security_disposition", ())
    codes = [item['code'] for item in dao.select("select distinct code from t_security_daily", ())]
    rows = dao.select("select code, date, pre_close, high, low, close, open from t_security_daily", ())
    code_rows_rel = {}
    for row in rows:
        code = row['code']
        if code not in code_rows_rel.keys():
            code_rows_rel.setdefault(code, [row])
        else:
            code_rows_rel[code].append(row)

    values = []
    for code in codes:
        rows = code_rows_rel[code]
        rows.sort(key=lambda x: x['date'], reverse=False)
        close_zt_count = 0
        high_zt_count = 0
        for item in rows:
            date = item['date']
            print("ValuesLen: " + str(values.__len__()) + " Code: " + code + " Date: " + date)
            pre_close = float(item['pre_close'])
            close = float(item['close'])
            high = float(item['high'])
            open = float(item['open'])
            c_rate = round((close - pre_close) / pre_close * 100, 2)
            h_rate = round((high - pre_close) / pre_close * 100, 2)
            o_rate = round((open - pre_close)/pre_close * 100, 2)
            if o_rate >= 9.89 and h_rate >= 9.89 and c_rate >= 9.89:
                continue
            if h_rate > 9.89:
                high_zt_count = high_zt_count + 1
                if h_rate == c_rate:
                    close_zt_count = close_zt_count + 1

        if high_zt_count == 0:
            success = 0
        else:
            success = round(close_zt_count/high_zt_count*100, 2)
        values.append((code, close_zt_count, success))

    print("saving 2 db ing...")
    dao.updatemany("insert into t_security_disposition(code, zt_count, success) values(%s,%s,%s)", values)


def testSuccess(sql_codes, values):
    codes = [item['code'] for item in dao.select(sql_codes, values)]
    high_zt_count = 0
    close_zt_count = 0
    for code in codes:
        df = ts.get_realtime_quotes(code)
        if df.__len__() == 0:
            continue
        high = float(df.high)
        open = float(df.open)
        price = float(df.price)
        pre_close = float(df.pre_close)

        o_rate = round((open - pre_close) / pre_close * 100, 2)
        h_rate = round((high - pre_close) / pre_close * 100, 2)
        c_rate = round((price - pre_close) / pre_close * 100, 2)

        if o_rate >= 9.89 and h_rate >= 9.89 and c_rate >= 9.89:
            continue
        if h_rate > 9.89:
            high_zt_count = high_zt_count + 1
            if h_rate == c_rate:
                close_zt_count = close_zt_count + 1



    if high_zt_count == 0:
        success = 0
    else:
        success = round(close_zt_count/high_zt_count*100, 2)

    print(success)


#refresh_BaseConceptAndIndustryData()
# refresh_BaseConceptRelationshipData()
#refreshSecurityDaily()
# refreshSecurityDisposition()

# testSuccess("SELECT code from t_security_disposition where success > 70 and zt_count > 2", ())
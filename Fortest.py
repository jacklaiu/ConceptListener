import Crawler as cr
import Dao as dao
import Util as util

def get_code_price(datestr, timestr):
    code_price = {}
    code_pre_close = {}
    code_open = {}
    arrFromDB = dao.select("select code, price, pre_close, open from t_security_timepoint_price where datestr=%s and timestr=%s", (datestr, timestr))
    if arrFromDB.__len__() > 0:
        for item in arrFromDB:
            code_price.setdefault(item['code'], item['price'])
            code_pre_close.setdefault(item['code'], item['pre_close'])
            code_open.setdefault(item['code'], item['open'])
        return {'code_price': code_price, 'code_pre_close': code_pre_close, 'code_open': code_open}

    #dao.update("delete from t_security_timepoint_price where datestr=%s and timestr=%s", (datestr,timestr))
    pre_open_date = util.preOpenDate(datestr)
    str_datestr = str(int(datestr.split('-')[1])) + "." + str(int(datestr.split('-')[2]))
    str_pre_open_datestr = str(int(pre_open_date.split('-')[1])) + "." + str(int(pre_open_date.split('-')[2]))
    soups = cr.getSoupsFromWencai(str_datestr+"日"+timestr+"价格；"+str_datestr+"日开盘价；"+str_pre_open_datestr+"日收盘价；a股；所属概念；所属同花顺行业；")
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
                index = index + 1
                o_str = es_code[index].text.strip()
                code_name.setdefault(code, o_str)
            index = index + 1

        es_c0 = soup.select('#resultWrap .scroll_tbody_con .tbody_table tr')
        count = 0
        for el in es_c0:
            a_el = el.select('td.item.sortCol div.em')[0]
            code = codes[count]
            if a_el.text.strip() == '--':
                code_price.setdefault(code, 0)
                print('Code: ' + code + '->Price: ' + str(0))
                continue
            price = float(a_el.text.strip())
            code_price.setdefault(code, price)
            print('Code: ' + code + '->Price: ' + str(price))
            count = count + 1

        es_c0 = soup.select('#resultWrap .scroll_tbody_con .tbody_table tr')
        count = 0
        for el in es_c0:
            a_el = el.select('td[colnum="5"] div.em')[0]
            code = codes[count]
            if a_el.text.strip() == '--':
                code_open.setdefault(code, 0)
                print('Code: ' + code + '->Open: ' + str(0))
                continue
            open = float(a_el.text.strip())
            code_open.setdefault(code, open)
            print('Code: ' + code + '->Open: ' + str(open))
            count = count + 1

        es_c0 = soup.select('#resultWrap .scroll_tbody_con .tbody_table tr')
        count = 0
        for el in es_c0:
            a_el = el.select('td[colnum="6"] div.em')[0]
            code = codes[count]
            if a_el.text.strip() == '--':
                code_pre_close.setdefault(code, 0)
                print('Code: ' + code + '->Pre_close: ' + str(0))
                continue
            pre_price = float(a_el.text.strip())
            code_pre_close.setdefault(code, pre_price)
            print('Code: ' + code + '->Pre_price: ' + str(pre_price))
            count = count + 1

    arr_values = []
    for code in code_price.keys():
        price = code_price[code]
        pre_close = code_pre_close[code]
        open = code_open[code]
        arr_values.append((code, price, pre_close, open, datestr, timestr))

    dao.updatemany("insert into t_security_timepoint_price(code, price, pre_close, open, datestr, timestr) values(%s,%s,%s,%s,%s,%s)", arr_values)
    return {'code_price': code_price, 'code_pre_close': code_pre_close, 'code_open': code_open}

#print(get_code_price('2018-08-03', '9:50'))
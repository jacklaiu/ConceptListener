import Crawler as cr
import Dao as dao
import Util as ut
import tushare as ts

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
    codes = [item['code'] for item in dao.select("select distinct code from t_security_daily", ())]
    values = []
    for code in codes:
        code_high_zt_count_rel = {}
        code_close_zt_count_rel = {}
        items = dao.select("select date, pre_close, high, low, close, open from t_security_daily where code=%s", (code))
        close_zt_count = 0
        high_zt_count = 0
        for item in items:
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

#refreshSecurityDaily()
refreshSecurityDisposition()

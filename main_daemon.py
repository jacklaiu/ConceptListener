import Util as util
import time
import Dao as dao
import tushare as ts

def daemon_fetchRealTimeSecurityData():
    now = util.getHMS()
    date = util.getYMD()
    codes = [item['code'] for item in dao.select("select distinct code from t_security_concept", ())]
    code_item_rel = {}
    while True:
        if now > '15:00:00' or now < '09:15:00' and util.isOpen(date) is False:
            time.sleep(5)
        count = 0
        while count < codes.__len__():
            dfs = ts.get_realtime_quotes(codes[count:count + 20])
            for row in dfs.iterrows():
                code = row[1]['code']
                code_item_rel.setdefault(code, row[1])
            print(codes[count:count + 20])
            count = count + 20

        for code in code_item_rel.keys():

            print()



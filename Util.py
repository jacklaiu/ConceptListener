import os
import tushare as ts
import time
import datetime

def getYMDHMS():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def getYMD():
    return time.strftime("%Y-%m-%d", time.localtime())

def getHMS():
    return time.strftime("%H:%M:%S", time.localtime())

def getTimeStamp():
    millis = int(round(time.time() * 1000))
    return millis

def getFormatToday():
    return time.strftime("%Y-%m-%d", time.localtime())

def getPreDayYMD(num=1, startdate=None):
    today=datetime.date.today()
    if startdate is not None:
        arr = startdate.split("-")
        today = datetime.date(int(arr[0]), int(arr[1]), int(arr[2]))
    oneday=datetime.timedelta(days=num)
    d=today-oneday
    return str(d)

def get_concept_securities():
    df = ts.get_concept_classified()
    values = df.values
    concept_code_dict = {}
    for row in values:
        code = row[0]
        c_name = row[2]
        if c_name not in concept_code_dict:
            concept_code_dict.setdefault(c_name, [code])
        else:
            concept_code_dict[c_name].append(code)
    return concept_code_dict


def initOpenDateTempFile():
    OpenList = ts.trade_cal()
    rows = OpenList[OpenList.isOpen == 1].values[-888:]
    f = open("temp_OpenDate.txt", "w")
    f.write("")
    f.close()
    f = open("temp_OpenDate.txt", "a")
    for row in rows:
        date = row[0]
        f.write(date + ";")
    f.close()

def getOpenDates():
    f = open(os.path.dirname(__file__) + "/temp_OpenDate.txt", "r")
    str = f.read()
    if str != "":
        dates = str.split(";")
    else: return None
    return dates

def get_k_data(df, start, end):
    ret = df[(df['date'] >= start) & (df['date'] <= end)]
    return ret

def isOpen(date):
    OpenDates = getOpenDates()
    str = ";".join(OpenDates)
    return date in str

def preOpenDate(date, leftCount=1):
    OpenDates = getOpenDates()
    index = 0
    for d in OpenDates:
        if d == date:
            return OpenDates[index - int(leftCount)]
        index = index + 1
    return None

def getLastestOpenDate(date=getYMD()):
    hms = getHMS()
    if hms >= '15:00:00' and isOpen(date):
        return date
    if hms < '15:00:00' and isOpen(date):
        return preOpenDate(date, 1)
    count = 0
    while True:
        count = count + 1
        if isOpen(date) == False:
            date = getPreDayYMD(1, date)
            continue
        else:
            break
    return date

def nextOpenDate(date, rightCount=1):
    OpenDates = getOpenDates()
    index = 0
    for d in OpenDates:
        if d == date:
            if index + rightCount < OpenDates.__len__() -1:
                return OpenDates[index + rightCount]
            else:
                break
        index = index + 1
    return None





# def getOpenDates():
#     OpenList = ts.trade_cal()
#     dates = []
#     rows = OpenList[OpenList.isOpen == 1].values[-888:]
#     for row in rows:
#         dates.append(row[0])
#     return dates
# def isOpen(date):
#     #OpenList = ts.trade_cal()
#     # try:
#     #     isOpen = OpenList[OpenList.calendarDate == date].values[0][1]
#     # except:
#     #     return None
#     # if (isOpen == 1):
#     #     return True
#     # return False
#     str = ";".join(OpenDates)
#     return date in str
# def get_today_open2close_chg(code, date=getYMD()):
#     try :
#         start = preOpenDate(date, 1)
#         end = start
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         dc = d['close']
#         do = d['open']
#         ye_open = do.values[0]
#         ye_close = dc.values[0]
#         ret = round(((float(ye_close) - float(ye_open)) / float(ye_open)), 4) * 100
#     except Exception as e:
#         print(e)
#         return None
#     return ret
#
# def get_ye_chg(code, date=getYMD()):
#     try:
#         start = preOpenDate(date, 2)
#         end = preOpenDate(date, 1)
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         dd = d['close']
#         ty_close = dd.values[0]
#         ye_close = dd.values[1]
#         ret = round(((float(ye_close) - float(ty_close)) / float(ty_close)), 4) * 100
#     except Exception as e:
#         print(e)
#         return None
#     return ret
#
# def get_ty_chg(code, date=getYMD()):
#     try:
#         start = preOpenDate(date, 3)
#         end = preOpenDate(date, 2)
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         dd = d['close']
#         ty_close = dd.values[0]
#         ye_close = dd.values[1]
#         ret = round(((float(ye_close) - float(ty_close)) / float(ty_close)), 4) * 100
#     except Exception as e:
#         print(e)
#         return None
#     return ret
#
# def get_continuous_rise_day_count(code, date=getYMD()):
#     count = 0
#     try:
#         #chg = get_ye_chg(code, preOpenDate(date, count))
#         chg = get_today_open2close_chg(code, preOpenDate(date, count))
#         while chg >= 0:
#             if count > 10:
#                 break
#             count = count + 1
#             #chg = get_ye_chg(code, preOpenDate(date, count))
#             chg = get_today_open2close_chg(code, preOpenDate(date, count))
#             if chg is None:
#                 break
#     except Exception as e:
#         print(e)
#         return None
#     return count
#
# def get_continuous_z_day_count(code, date=getYMD()):
#     count = 0
#     try:
#         chg = get_ye_chg(code, preOpenDate(date, count))
#         #chg = get_today_open2close_chg(code, preOpenDate(date, count))
#         while chg >= 0:
#             if count > 10:
#                 break
#             count = count + 1
#             chg = get_ye_chg(code, preOpenDate(date, count))
#             #chg = get_today_open2close_chg(code, preOpenDate(date, count))
#             if chg is None:
#                 break
#     except Exception as e:
#         print(e)
#         return None
#     return count
#
# def get_ye_qrr(code, date=getYMD()):
#     try:
#         start = preOpenDate(date, 6)
#         end = preOpenDate(date, 1)
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         dd = d['volume']
#         len = dd.values.__len__()
#         total_volume = 0
#         ye_volume = 0
#         count = 0
#         for row in dd.values:
#             if count == len - 1:
#                 ye_volume = row
#             else:
#                 total_volume = total_volume + row
#             count = count + 1
#         base = total_volume / (4*60*5)
#         today = ye_volume / (4*60)
#         if base == 0:
#             return None
#         ret = round(float(today/base), 2)
#     except Exception as e:
#         print(e)
#         return None
#     return ret
#
# # def get_ye_tr(code, date=getYMD()):
# #     start = preOpenDate(date, 1)
# #     end = preOpenDate(date, 1)
# #     if isinstance(code, str):
# #         d = ts.get_k_data(code=code, start=start, end=end)
# #     else:
# #         d = get_k_data(df=code, start=start, end=end)
# #         code = d['code'].values[0]
# #         d = ts.get_hist_data(code, start, end)
# #     dd = d['turnover']
# #     print()
#
# def get_open_chg(code, date=getYMD()):
#     try:
#         start = preOpenDate(date, 1)
#         end = date
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         pre_close = d['close'].values[0]
#         open = d['open'].values[1]
#         ret = round(((float(open) - float(pre_close)) / float(pre_close)), 4) * 100
#     except Exception as e:
#         print(e)
#     return ret
#
# def get_close_chg(code, date=getYMD()):
#     try:
#         start = preOpenDate(date, 1)
#         end = date
#         if isinstance(code, str):
#             d = ts.get_k_data(code=code, start=start, end=end)
#         else:
#             d = get_k_data(df=code, start=start, end=end)
#         pre_close = d['close'].values[0]
#         close = d['close'].values[1]
#         ret = round(((float(close) - float(pre_close)) / float(pre_close)), 4) * 100
#     except Exception as e:
#         print(e)
#     return ret

# dataf = ts.get_k_data('000565', start="2018-07-06", end="2018-07-20")
# print("open_chg: " + str(get_open_chg(dataf, "2018-07-20")))
# print("close_chg: " + str(get_close_chg(dataf, "2018-07-20")))

#initOpenDateTempFile()
# print(isOpen("2018-07-21"))
# print(os.path.dirname(__file__))

# -*- coding: gbk -*-
from flask import Flask, abort, request, jsonify
import Dao as dao
import tushare as ts
app = Flask(__name__, static_url_path='')
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    concept_codes_rel = {}
    code_item_rel = {}
    code_basics_rel = {}
    concept_chg_rel = {}
    df_basics = ts.get_stock_basics()
    for row in df_basics.iterrows():
        code = row[0]
        code_basics_rel.setdefault(code, row[1]) if code not in code_basics_rel.keys() else code_basics_rel[code].append(row[1])

    print()
    # app.run()
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
        count = count + 20

    ##concept_chg_rel################################################
    for concept in concept_codes_rel.keys():
        codes = concept_codes_rel[concept]
        item = code_item_rel[code]










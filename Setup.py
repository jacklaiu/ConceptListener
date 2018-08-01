import Crawler as cr
import Dao as dao

# 刷新数据库内股票对应的板块+概念
def refreshSecurityConcepts():
    dao.update("truncate table t_security_concept", ())
    soups = cr.getSoupsFromWencai("a股；所属概念；所属同花顺行业；")
    code_concepts = {}
    code_name = {}
    for soup in soups:
        es_code = soup.select('#resultWrap .static_con_outer .tbody_table tr td.item div.em')
        index = 0
        codes = []
        names = []
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

        es_c1 = soup.select('#tableWrap .scroll_tbody_con .scroll_table tr')
        count = 0
        for el in es_c1:
            a_els = el.select('td[colnum="6"] div span.fl a')
            code = codes[count]
            for a_el in a_els:
                concept = a_el.text.strip()
                code_concepts[code].append(concept)
                print('Code: ' + code + '->Concept: ' + concept)
            count = count + 1

    values = []
    for code in code_concepts.keys():
        concepts = code_concepts[code]
        for concept in concepts:
            name = code_name[code]
            values.append((code, name, concept))

    dao.updatemany('insert into t_security_concept(code, name, concept) values(%s,%s,%s)', values)

refreshSecurityConcepts()
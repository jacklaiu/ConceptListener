import Crawler as cr
import Dao as dao

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

#refresh_BaseConceptAndIndustryData()
refresh_BaseConceptRelationshipData()
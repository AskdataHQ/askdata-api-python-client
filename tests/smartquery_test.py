import jsons
from askdata.smartquery import *

if __name__ == '__main__':
    field1 = Field('revenue', 'MAX')
    from1 = From('dataframe')
    condition1 = Condition(field1, '>=', ["2020-11-01 00:00:00"])
    condition2 = Condition(field1, '<=', ["2020-11-30 23:59:59"])
    condition3 = Condition(Field('player'), '==', ["Cristiano Ronaldo"])
    group1 = GroupElement('month')
    sorting1 = Sorting('revenue', SQLSorting.DESC)
    query1 = Query(fields=[field1], froms=[from1], where=[condition1, condition2, condition3], groupBy=[group1],
                   orderBy=[sorting1], limit=10)
    smartquery = SmartQuery([query1])
    dump = jsons.dumps(smartquery, strip_nulls=True)
    print(dump)
    print(smartquery.queries[0].to_sql())

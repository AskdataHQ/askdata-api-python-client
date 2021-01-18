import jsons
from askdata.smartquery import *

if __name__ == '__main__':
    field1 = Field(column='{{measure.A}}', aggregation='MAX', dataset='{{dataset.A}}', entityType="measure")
    field2 = Field(column='{{dimension.A}}', dataset='{{dataset.B}}', entityType="dimension")
    field3 = Field(column='{{timeDimension.A}}', dataset='{{dataset.C}}', entityType="timeDimension")
    from1 = From('{{dataset.A}}')
    from2 = From('{{dataset.B}}')
    from3 = From('{{dataset.C}}')
    condition1 = Condition(field3, SQLOperator.GOE, ["{{timePeriod.A}}"])
    condition2 = Condition(field3, SQLOperator.LOE, ["{{timePeriod.B}}"])
    condition3 = Condition(field1, SQLOperator.EQ, [2000])
    sorting1 = Sorting("{{measure.A}}", SQLSorting.DESC)
    query1 = Query(fields=[field1, field2, field3], datasets=[from1, from2, from3], where=[condition1, condition2, condition3],
                   orderBy=[sorting1], limit=10)
    smartquery = SmartQuery([query1])
    dump = jsons.dumps(smartquery, strip_nulls=True)
    print(dump)
    print(smartquery.queries[0].to_sql())

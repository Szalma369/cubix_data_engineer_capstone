import pyspark.sql.functions as F
from pyspark.sql import DataFrame

SALES_MAPPING = {
    'son': 'SalesOrderNumber',
    'orderdate': 'OrderDate',
    'pk': 'ProductKey',
    'ck': 'CustomerKey',
    'dateofshipping': 'ShipDate',
    'oquantity': 'OrderQuantity'
}


def get_sales(sales_raw: DataFrame) -> DataFrame:
    '''
    Map and filter Sales data.

    :param sales_raw:   Raw Sales data.
    :return:            Mapped and filtered Sales data.
    '''

    return (
        sales_raw
        .select(
            F.col('son').cast('string'),
            F.col('orderdate').cast('date'),
            F.col('pk').cast('int'),
            F.col('ck').cast('int'),
            F.col('dateofshipping').cast('date'),
            F.col('oquantity').cast('int')
        )
        .withColumnRenamed(SALES_MAPPING)
        .dropDuplicates()
    )

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

PRODUCTS_MAPPING = {
    'pk': 'ProductKey',
    'psck': 'ProductSubCategoryKey',
    'name': 'ProductName',
    'stancost': 'StandardCost',
    'dealerprice': 'DealerPrice',
    'listprice': 'ListPrice',
    'color': 'Color',
    'size': 'Size',
    'range': 'SizeRange',
    'weight': 'Weight',
    'nameofmodel': 'ModelName',
    'ssl': 'SafetyStockLevel',
    'desc': 'Description'
}


def get_products(products_raw: DataFrame) -> DataFrame:
    '''
    Transform and filter Products data.

    1. Select needed columns and cast data types.
    2. Apply the column name mapping.
    3. Create ProfitMargin column.
    4. Replace 'NA' values with None.
    5. Drop duplicates.

    :param products_raw:    Raw Products data
    :return:                Cleaned, filtered and transformed Products data
    '''

    return (
        products_raw
        .select(
            F.col('pk').cast('int'),
            F.col('psck').cast('int'),
            F.col('name').cast('string'),
            F.col('stancost').cast(DecimalType(10, 2)),
            F.col('dealerprice').cast(DecimalType(10, 2)),
            F.col('listprice').cast(DecimalType(10, 2)),
            F.col('color').cast('string'),
            F.col('size').cast('int'),
            F.col('range').cast('string'),
            F.col('weight').cast(DecimalType(10, 2)),
            F.col('nameofmodel').cast('string'),
            F.col('ssl').cast('int'),
            F.col('desc').cast('string')
        )
        .withColumnsRenamed(PRODUCTS_MAPPING)
        .withColumn(
            'ProfitMargin',
            F.col('ListPrice') - F.col('DealerPrice')
        )
        .replace(
            'NA',
            None
        )
        .dropDuplicates()
    )

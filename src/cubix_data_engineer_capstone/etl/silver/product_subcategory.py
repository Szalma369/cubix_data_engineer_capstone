import pyspark.sql.functions as F
from pyspark.sql import DataFrame

PRODUCT_SUBCATEGORY_MAPPING = {
    'psk': 'ProductSubCategoryKey',
    'pck': 'ProductCategoryKey',
    'epsn': 'EnglishProductSubcategoryName',
    'spsn': 'SpanishProductSubcategoryName',
    'fpsn': 'FrenchProductSubcategoryName'
}


def get_product_subcategory(product_subcategory_raw: DataFrame) -> DataFrame:
    '''
    Transform and filter Product Subcategory data.

    1. Select needed columns and cast data types.
    2. Apply the column name mapping.
    3. Drop duplicates.

    :param product_subcategory_raw:    Raw Product Subcategory data
    :return:                            Cleaned, filtered and transformed Product Subcategory data  # noqa: 501
    '''

    return (
        product_subcategory_raw
        .select(
            F.col('psk').cast('int'),
            F.col('pck').cast('int'),
            F.col('epsn').cast('string'),
            F.col('spsn').cast('string'),
            F.col('fpsn').cast('string')
        )
        .withColumnsRenamed(PRODUCT_SUBCATEGORY_MAPPING)
        .dropDuplicates()
    )

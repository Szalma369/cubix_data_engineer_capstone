import pyspark.sql.functions as F
from pyspark.sql import DataFrame

PRODUCT_CATEGORY_MAPPING = {
    'pck': 'ProductCategoryKey',
    'epcn': 'EnglishProductCategoryName',
    'spcn': 'SpanishProductCategoryName',
    'fpcn': 'FrenchProductCategoryName'
}


def get_product_category(product_category_raw: DataFrame) -> DataFrame:
    '''
    Transform and filter Product Category data.

    1. Select needed columns and cast data types.
    2. Apply the column name mapping.
    3. Drop duplicates.

    :param product_category_raw:        Raw Product Category data
    :return:                            Cleaned, filtered and transformed Product Category data
    '''

    return (
        product_category_raw
        .select(
            F.col('pck').cast('int'),
            F.col('epcn').cast('string'),
            F.col('spcn').cast('string'),
            F.col('fpcn').cast('string')
        )
        .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
        .dropDuplicates()
    )

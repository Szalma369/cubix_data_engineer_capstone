import pyspark.sql.functions as F
from pyspark.sql import DataFrame

CUSTOMERS_MAPPING = {
    'ck': 'CustomerKey',
    'name': 'Name',
    'bdate': 'BirthDate',
    'ms': 'MaritalStatus',
    'gender': 'Gender',
    'income': 'YearlyIncome',
    'childrenhome': 'NumberChildrenAtHome',
    'occ': 'Occupation',
    'hof': 'HouseOwnerFlag',
    'nco': 'NumberCarsOwned',
    'addr1': 'AddressLine1',
    'addr2': 'AddressLine2',
    'phone': 'Phone'
}


def get_customers(customers_raw: DataFrame) -> DataFrame:
    '''
    Transform and filter Customers data.

    1. Select needed columns.
    2. Apply the column name mapping.
    3. Transform MaritalStatus.
    4. Transform Gender.
    5. Create FullAddress column.
    6. Create IncomeCategory column.
    7. Create BirthYear column.
    8. Drop duplicates.

    :param customers_raw:   Raw Customers data
    :return:                Cleaned, filtered and transformed Customers data
    '''

    return (
        customers_raw
        .select(
            F.col('ck').cast('int'),
            F.col('name').cast('string'),
            F.col('bdate').cast('date'),
            F.col('ms').cast('string'),
            F.col('gender').cast('string'),
            F.col('income').cast('int'),
            F.col('childrenhome').cast('int'),
            F.col('occ').cast('string'),
            F.col('hof').cast('int'),
            F.col('nco').cast('int'),
            F.col('addr1').cast('string'),
            F.col('addr2').cast('string'),
            F.col('phone').cast('string')
        )
        .withColumnsRenamed(CUSTOMERS_MAPPING)
        .withColumn(
            'MaritalStatus',
            F.when(F.col('MaritalStatus') == 'M', 1)
            .when(F.col('MaritalStatus') == 'S', 0)
            .otherwise(None)
            .cast('int')
        )
        .withColumn(
            'Gender',
            F.when(F.col('Gender') == 'M', 1)
            .when(F.col('Gender') == 'F', 0)
            .otherwise(None)
            .cast('int')
        )
        .withColumn(
            'FullAddress',
            F.concat_ws(', ', F.col('AddressLine1'), F.col('AddressLine2'))
        )
        .withColumn(
            'IncomeCategory',
            F.when(F.col('YearlyIncome') <= 50000, 'Low')
            .when(F.col('YearlyIncome') <= 100000, 'Medium')
            .otherwise('High')
        )
        .withColumn(
            'BirthYear',
            F.year(F.col('BirthDate'))
            .cast('int')
        )
        .dropDuplicates()
    )

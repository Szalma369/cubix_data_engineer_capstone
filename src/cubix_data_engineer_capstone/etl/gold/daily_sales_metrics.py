import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def get_daily_sales_metrics(wide_sales: DataFrame) -> DataFrame:
    '''
    Calculates daily sales metrics from the wide_sales DataFrame.

    Note: In order to get only two decimals for the averages, value is rounded.

    :param wide_sales: Input DataFrame containing wide sales data.
    :return:           DataFrame with daily metrics including 'SalesAmountSum', 'SalesAmountAvg',
                       "ProfitSum', and 'ProfitAvg' grouped by 'OrderDate'.
    '''

    return (
        wide_sales
        .groupBy(F.col('OrderDate'))
        .agg(
            F.sum(F.col('SalesAmount')).alias('SalesAmountSum'),
            F.round(F.avg(F.col('SalesAmount')), 2).alias('SalesAmountAvg'),
            F.sum(F.col('Profit')).alias('ProfitSum'),
            F.round(F.avg(F.col('Profit')), 2).alias('ProfitAvg')
        )
    )

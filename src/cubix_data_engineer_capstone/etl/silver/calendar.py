import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def get_calendar(calendar_raw: DataFrame) -> DataFrame:
    '''
    Clean and transform data type for Calendar data.

    1. Select required columns.
    2. Cast them explicitly.
    3. Drop duplicates.

    :param calendar_raw: Raw Calendar DataFrame.
    :return: Transformed Calendar DataFrame.
    '''

    return (
        calendar_raw
        .select(
            F.col('Date').cast('date'),
            F.col('DayNumberOfWeek').cast('int'),
            F.col('DayName').cast('string'),
            F.col('MonthName').cast('string'),
            F.col('MonthNumberOfYear').cast('int'),
            F.col('DayNumberOfYear').cast('int'),
            F.col('WeekNumberOfYear').cast('int'),
            F.col('CalendarQuarter').cast('int'),
            F.col('CalendarYear').cast('int'),
            F.col('FiscalYear').cast('int'),
            F.col('FiscalSemester').cast('int'),
            F.col('FiscalQuarter').cast('int'),
            F.col('FinMonthNumberOfYear').cast('int'),
            F.col('DayNumberOfMonth').cast('int'),
            F.col('MonthID').cast('int')
        )
        .dropDuplicates()
    )

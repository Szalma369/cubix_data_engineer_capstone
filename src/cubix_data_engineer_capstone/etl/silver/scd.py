from delta.tables import DeltaTable  # type: ignore
from pyspark.sql import DataFrame, SparkSession


def scd1_uc(spark: SparkSession, table_name: str, new_data: DataFrame, primary_key: str):
    '''
    Slowly changing dimension type 1 for UC Volumes.
    Compares the master Delta table with new data, updating or inserting as needed.

    :param spark:       SparkSession.
    :param table_name:  Name of the dimension table.
    :param new_data:    DataFrame with the new data.
    :param primary_key: Column name used as primary key.
    '''

    delta_master = (
        DeltaTable
        .forName(
            spark,
            table_name
        )
    )

    (
        delta_master
        .alias('master')
        .merge(
            new_data.alias('new_data'),
            f'master.{primary_key} = new_data.{primary_key}'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

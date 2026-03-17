"""
First import the modules that are necessary for the code
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    """
    Data quality class 
    """
    
    def __init__(self, df):
        
        self.df = df
        
        
    #Method for creating an instance of the class from a csv file
    @classmethods
    def createfrom_csv(cls, spark, file_path: str):
        df = spark.read.load(file_path,
                                format="csv", 
                                 sep=",", 
                                 inferSchema="true", 
                                 header="true")
        return cls(df)
    
    
    #Method for creating an instance of the class from a pandas dataframe
    @classmethods
    def createfrom_pandas(cls, spark, pd_df: pd.DataFrame):
        df = spark.CreateDataFrame(pd_df)
        return cls(df)
    
    #Validation methods creation
    
    def create_range_boolean(self, column, lower = None, upper = None):
        dict_dtypes = dict(self.df.dtypes)
        col_type = dict_dtypes.get(column)
        
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        if col_type is not None and col_type.lower() in num_types:
            if lower is not None and upper is not None:
                self.df = self.df.withColumn("Is_in_range", 
                   when((self.df[column]>=lower & self.df[column]<=upper), True)
                  .when(self.df[column].isNull(), F.lit(None).cast('boolean'))
                  .otherwise(False))
                return self.df
            elif lower is not None and upper is None:
                self.df = self.df.withColumn("Is_in_range", 
                   when(self.df[column]>=lower, True)
                  .when(self.df[column].isNull(), F.lit(None).cast('boolean'))
                  .otherwise(False))
                return self.df
            elif upper is not None and lower is None:
                self.df = self.df.withColumn("Is_in_range", 
                   when(self.df[column]<=upper, True)
                  .when(self.df[column].isNull(), F.lit(None).cast('boolean'))
                  .otherwise(False))
                return self.df
        else:
            print(f'The column {column} is not a numeric column')
            return self.df
        return self.df
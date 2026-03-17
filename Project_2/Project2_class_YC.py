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
    def createfrom_csv(cls, spark, file_path):
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
    
    

    
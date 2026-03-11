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
    
    """
    
    def __init__(self, df):
        
        self.df = df
    

    
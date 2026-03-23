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
        self.mm = pd.DataFrame()
        self.n = pd.DataFrame()
        
        
    #Method for creating an instance of the class from a csv file
    @classmethod
    def createfrom_csv(cls, spark, file_path: str):
        df = spark.read.load(file_path,
                                format="csv", 
                                 sep=",", 
                                 inferSchema="true", 
                                 header="true")
        return cls(df)
    
    
    #Method for creating an instance of the class from a pandas dataframe
    @classmethod
    def createfrom_pandas(cls, spark, pd_df: pd.DataFrame):
        df = spark.CreateDataFrame(pd_df)
        return cls(df)
    
    #Validation methods creation
    
    #Creating a column of boolean that verify if the values of a numeric column are in the range of a what a user especifies.
    def create_range_boolean(self, column, lower = None, upper = None):
        
        #A dictionary with the columns and their type is created
        dict_dtypes = dict(self.df.dtypes) 
        
        #The type of the column on which it is going to be verified is extracted and stored in a new variable
        col_type = dict_dtypes.get(column) 
        
        #A list with the numeric types of variables is created
        num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
        
        #We verify if the column is introduced, and that its type is numeric
        if col_type is not None and col_type.lower() in num_types:
            
            #If it is numeric, we check wheter both limits have been input to the method
            if lower is not None and upper is not None:
                
                #If both limits were input, will check for if they are in the range then create a boolean column indicating which fall in the range
                self.df = self.df.withColumn("Is_in_range", 
                  F.when(self.df[column].isNull(), None)
                  .when((self.df[column]>=lower) & (self.df[column]<=upper), True)
                  .otherwise(False))
              
            
            elif lower is not None and upper is None:
                
                #If just the lower limit is provided, will check just that bound and generate the boolean column
                self.df = self.df.withColumn("Is_in_range", 
                  F.when(self.df[column].isNull(), None)
                  .when(self.df[column]>=lower, True)
                  .otherwise(False))
             
            
            elif upper is not None and lower is None:
                
                #If just the upper limit is provided, will check just that bound and generate a boolean column that meet the conditions
                self.df = self.df.withColumn("Is_in_range",                 
                  F.when(self.df[column].isNull(), None)
                  .when(self.df[column]<=upper, True)
                  .otherwise(False))
               
        
        else:
            
            #If the column is not numeric, will print the message and return the dataframe without boolean column
            print(f'The column {column} is not a numeric column')
            return self.df
        return self.df
    
    #A method that checks if each value in a string column falls within a user specified set of levels and returns the dataframe with an appended column of Boolean values.
    def create_string_boolean(self, column, str_list: list):
        dict_dtypes = dict(self.df.dtypes) 
        
        #The type of the column on which it is going to be verified is extracted and stored in a new variable
        col_type = dict_dtypes.get(column) 
             
        #We verify if the column is introduced, and that its type string
        if col_type is not None and col_type.lower() == "string":
            
            #If the column is string type we check if the values supplied are in the column and create a boolean column
            self.df = self.df.withColumn(f"Is_in_{column}", 
                F.when(self.df[column].isNull(), None)
                 .when(self.df[column].isin(str_list), True)
                 .otherwise(False))
         
        else:
            
            #If the column is not numeric, will print the message and return the dataframe without boolean column
            print(f'The column {column} is not a string column')

        return self.df
    
    #A method that check if a eachvalue in a column is missing(NULL specifically) and create a column of boolean values
    def create_missing(self, column):
        
        #Checking if the value doesn't exist, then the boolean for that row will be true
        self.df = self.df.withColumn("Is_missing", 
            F.when(self.df[column].isNull(), True)
            .otherwise(False))
        return self.df
    
    #Methods for summaries
    
    #Method to report min and max of a numeric column supplied by the user
    def get_min_max(self, column = None, group = None):
        
        #When a value for column is supplied
        if column is not None:
             #A dictionary with the columns and their type is created
            dict_dtypes = dict(self.df.dtypes) 

            #The type of the column on which it is going to be verified is extracted and stored in a new variable
            col_type = dict_dtypes.get(column) 

            #A list with the numeric types of variables is created
            num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']

            #We verify that its type is numeric
            if col_type is not None and col_type.lower() in num_types:
                
                #We verify that the group variable is not None for the first case
                if group is not None:
                    
                    #If the group is defined when calling the method
                    self.mm = (self.df.groupBy(group)
                                .agg(F.max(column).alias(f"max_{column}"),
                                     F.min(column).alias(f"min_{column}"))
                                .toPandas())
                    
                
                else:
                    
                    #If the group is not especified
                    self.mm = (self.df.agg(F.max(column).alias(f"max_{column}"),
                                     F.min(column).alias(f"min_{column}"))
                                 .toPandas())
                    
                
            else:
                
                #If the column type is not numeric
                print(f'The column {column} is not a numeric column')
                return None
         
        #When the column is not supplied
        else:
            if group is not None:
                    
                #If the group is defined when calling the method
                    
                #This will take the numeric columns from the table
                numeric_cols = [c for c, t in self.df.dtypes if t in ("int", "integer", "bigint", "float", "double", "long")]
                
                dfs = []
                for i in numeric_cols:
                    pdf = (self.df.groupBy(group)
                               .agg(F.min(i).alias(f"min_{i}"),
                                    F.max(i).alias(f"max_{i}"))
                               .toPandas())
                    
                    dfs.append(pdf)
                
                self.mm = reduce(
                    lambda left, right: pd.merge(left, right, on=group, how="outer"),
                    dfs)
                
                
            else:
                
                #When group and column is not an imput, this will calculate min and max for all the columns
                numeric_cols = [c for c, t in self.df.dtypes if t in ("int", "integer", "bigint", "float", "double", "long")]
                
                dfs = []
                for i in numeric_cols:
                    pdf = (self.df.agg(F.min(i).alias("min"), 
                                       F.max(i).alias("max"))
                                  .toPandas().T)
                    
                    
                    pdf.columns = [i]
                    
                    pdf["stat"] = ["min", "max"]
                    dfs.append(pdf)
                
                self.mm = reduce(
                        lambda left, right: pd.merge(left, right, on="stat", how="outer"),
                        dfs
                        ).set_index("stat")
                    
        return self.mm
    
    #Method to report the counts associated with one or two string columns.
    
    def get_counts(self, col1, col2 = None):
        
            #Checking that the col1 is not numeric
            dict_dtypes = dict(self.df.dtypes) 
        
            #The type of the column on which it is going to be verified is extracted and stored in a new variable
            col1_type = dict_dtypes.get(col1) 


            #A list with the numeric types of variables is created
            num_types = ['float', 'int', 'longint', 'bigint', 'double', 'integer']
                #A dictionary with the columns and their type is created

            if col2 is None:

                if col1_type.lower() not in num_types:
                    self.n = (self.df
                                  .groupBy(col1)
                                  .agg(F.count("*").alias("Count"))
                                 ).toPandas()
                else:

                    print(f"The column {col1} is numeric")

                    return None

            else:
                col2_type = dict_dtypes.get(col2)
                if col2_type.lower() not in num_types and col1_type.lower() not in num_types:

                    #Counts when the two columns are introduced
                    self.n = (self.df
                                  .groupBy(col1, col2)
                                  .agg(F.count("*").alias("Count"))
                                 ).toPandas()
                else:
                    print(f"{col1} or {col2} or both are numeric")

                    return None
            return self.n
                      
            
            
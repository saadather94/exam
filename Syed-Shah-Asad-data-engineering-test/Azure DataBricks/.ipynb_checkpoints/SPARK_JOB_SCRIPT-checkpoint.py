
## Importing Libraries and creating spark session

import pyspark
from pyspark.sql import SparkSession as ss
from pyspark.sql.functions import regexp_extract, regexp_replace,avg, udf
from pyspark.sql.functions import round as Round
from pyspark.sql.functions import col, when
from pyspark.sql.functions import to_date
from pyspark.sql.functions import lower
from pyspark.sql.types import IntegerType
import re
from statistics import mean


spark = ss.builder.appName('ADDO-EXAM').getOrCreate()
sc= spark.sparkContext


# For this task I had uploaded the recipe files to my Azure storage and performed operations 
# by reading them from the Azure blob storage, since in real time enviroment the data is being stored 
# on the cloud storage so it make sense to do this way by storing files on cloud and then performing spark tasks.


# Authentication to access the files on storage
storage_account_name = "sparkexamstorage"  
storage_account_access_key = "QmRcItJlh4AE8ba/9x8sQSs2D6DhsusEf2CtNIiZBQRCBP84mRJR8eprOFGjeNtn5xzqzpAZu1XS+AStyAzBWg=="

# Containers on Azure Storage
inp_container = "data" ## Container name where recipies are present 
inp_file_type = "json"

out_container = "output" ## Container name where final result file which contains only the beef recipies to be stored
out_file_type = "csv"

# Configuring Spark Application connected with azure storage
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# Location of recipie files stored on Azure Storage container
input_loc = dbutils.fs.ls(f"wasbs://{inp_container}@{storage_account_name}.blob.core.windows.net/")

#Location of output container
output_container_path = f"wasbs://{out_container}@{storage_account_name}.blob.core.windows.net/"


# Since all the column are in string format and need to be corected:
# > Time is given in ISO format need to extract time in minutes.
# > Dish per person serving needs to be extracted from recipeYield columns 
# > Filling/removing of Null record.
# > Removing extra spaces/punctuations from description and ingredients columns.
# 
# Therefore for sucessfull task completion for that some functions are needed to be defined as:


###########################################################
#### Function to extract the Time per dish in minutes #####
###########################################################

def get_time(ISOtime_):
    hours = 0
    minutes = 0
    
    # extract hours, based on regular expression
    hours_match = re.search(r'(\d+)H', ISOtime_)
    if hours_match:
        hours = int(hours_match.group(1))
        
    # extract minutes, based on regular expression
    minutes_match = re.search(r'(\d+)M', ISOtime_)
    if minutes_match:
        minutes = int(minutes_match.group(1))
        
    # Calculate total minutes
    total_minutes = hours * 60 + minutes
    return total_minutes

###################################################
#### Function to extract the servings per dish #####
###################################################

def extract_serving_value(s):
    num_regex = re.compile(r'\d+')
    
    # If a single entity is found, then return it
    if len(s)==1 and isinstance(s,int):
        return (s)
    else:
        # If a string is like '4 to 5 serving' etc then return the average serving 
        # and return 1 if no serving is found.
        nums = [int(num) for num in num_regex.findall(s)]
        return (mean(nums)) if nums else (1)



###########################################################
##### TASK 1 ##############################################
########################################################### 

def PrePocessing(df):

  # Selecting the columns which is useful to us
  df2 = df.select("datePublished","name","cookTime","ingredients","prepTime","recipeYield")

  ####################################
  ##### Operation on datePublished ###
  ####################################

  # Converting the date to Date format from string
  df2 = df2.withColumn("Date Published", to_date(df2["datePublished"]))


  ####################################
  ##### Operation on Name Column #####
  ####################################


  # Remove extra spaces, punctuations and lower casing from the 'name' column
  df2 = df2.withColumn('Dish_Name', regexp_replace('name', '[^\w\s]+', '').alias('Name'))
  df2 = df2.withColumn('Dish_Name', regexp_replace('name', '\s+', ' ').alias('Name'))
  df2 = df2.withColumn('Dish_Name', lower(df2['name']))


  ####################################
  ##### Operation on Ingredients #####
  ####################################


  # Remove extra spaces, punctuations and lower casing from the 'ingredients' column for future search.
  df2 = df2.withColumn('Ingridients', regexp_replace('ingredients', '[^\w\s]+', '').alias('Ingridients'))
  df2 = df2.withColumn('Ingridients', regexp_replace('ingredients', '\s+', ' ').alias('Ingridients'))
  df2 = df2.withColumn('Ingridients', lower(df2['ingredients']))


  ####################################
  ##### Operation on Cook Time #######
  ####################################

  # Getting cook time in minutes 
  cooktime_func = udf(get_time, IntegerType())

  # Applying the operation to cooktime column
  df2 = df2.withColumn('Cook_Time', cooktime_func(df2['cookTime']))

  # Replace null values with the mean of previous values
  mean_val_CT = df2.select(avg(col('Cook_Time'))).collect()[0][0]
  df2 = df2.fillna({'Cook_Time': mean_val_CT})

  # Round the values of newly added cook and prep columns
  df2 = df2.withColumn('Cook_Time', Round(col('Cook_Time'), 0))


  ####################################
  ##### Operation on Prep Time #######
  ####################################

  # Getting Prep time in minutes 
  preptime_func = udf(get_time, IntegerType())

  # Applying the operation to preptime column
  df2 = df2.withColumn('Prep_Time', preptime_func(df2['prepTime']))

  # Replace null values with the mean of previous values
  mean_val_PT = df2.select(avg(col('Prep_Time'))).collect()[0][0]
  df2 = df2.fillna({'Prep_Time': mean_val_PT})

  # Round the values of newly added cook and prep columns
  df2 = df2.withColumn('Prep_Time', Round(col('Prep_Time'), 0))

  ####################################
  ##### Operation on recipeYield #####
  ####################################


  # Getting the Serving per dish 
  serving_func = udf(extract_serving_value, IntegerType())
  df2 = df2.withColumn('Serving', serving_func(df2['recipeYield']))

  # Round the values of newly added cook and prep columns
  df2 = df2.withColumn('Serving', Round(col('Serving'), 0))



  # Drop the old columns
  df2 = df2.drop('cookTime')
  df2 = df2.drop('prepTime')
  df2 = df2.drop('name')
  df2 = df2.drop('datePublished')
  df2 = df2.drop('recipeYield')
  df2 = df2.drop('ingredients')

  return df2


###########################################################
##### TASK 2 ##############################################
########################################################### 

def BeefRecipies(df2):


  # Create a new column by adding two existing columns
  df2 = df2.withColumn('Total_cook_time', col('Cook_Time') + col('Prep_Time'))

  # Create a new column based on the total cook time,
  df2 = df2.withColumn('Difficulty', when(col('Total_cook_time') < 30, 'Easy')
                                    .when((col('Total_cook_time') >= 30) & (col('Total_cook_time') <= 60), 'Medium')
                                    .when(col('Total_cook_time') > 60, 'Hard'))



  # Creating a table EXAM from dataframe, for quering the beef from record
  df2.createOrReplaceTempView('EXAM')

  # SQL query that shows the dishes having beef in their ingredites or in dish name 
  beef_recipe = spark.sql("select * from EXAM where (Ingridients like '%beef%') or (Dish_Name like '%beef%')")

  return beef_recipe

################################################################################################



df = {}
# Iterate over all the saved records on the cloud storage and perform operations based on our tasks requirement.
for l in range(len(input_loc)):

  # Getting the file name and creating a dataframe according to its name.
  file_location = input_loc[l][0]
  df[file_location[-16:-5]] = spark.read.format(inp_file_type).load(file_location)


  # ##### TASK 1 
  df2 = PrePocessing(df[file_location[-16:-5]]) 

  # Persist the dataframe for future processing
  df[file_location[-16:-5]] = df2.persist()

  # ##### TASK 2
  df2 = BeefRecipies(df[file_location[-16:-5]])


  ##### Writing the final output to the Azure cloud storage  
  output_folder = f"wasbs://{out_container}@{storage_account_name}.blob.core.windows.net/Output-{file_location[-16:-5]}/"
  (df2.coalesce(1).write.mode("overwrite").option("header", "true").format(out_file_type).save(output_folder))

  # Get the name of the CSV file that was just saved to Azure blob storage
  files = dbutils.fs.ls(output_folder)
  output_file = [x for x in files if x.name.startswith("part-")]

  # Renameing the file name since the save file has a very long name
  dbutils.fs.mv(output_file[0].path,f"{output_container_path}/Output-{file_location[-16:-5]}/Final-Recipee-{file_location[-8:-5]}.csv")

########################################################################################################################################

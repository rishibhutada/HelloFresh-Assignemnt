from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when
from pyspark.sql import functions as F
import os, glob
import configparser
import util


def get_minutes(df_to_be_formatted,prep_time):
    '''
    Converting Prep Time in numerical format to perform mathematical operation over it
    :param prep_time:
    :return: Time
    '''
    #Checking for String length and deciding course of action to take as follows:
    #Length is 4 and M is last Character 'PT5M' -> 5
    #Length is 4 and H is last Character 'PT4H' -> 240
    #Length is 5 'PT25M' -> 25
    #Length is 6 'PT5H5M' -> 305 ((5*60) + 5)
    #Length is 7 'PT5H25M' -> 325 ((5*60) + 25
    #Other than that 'PT' -> 0
    df_to_be_formatted = df_to_be_formatted.withColumn(prep_time, F.when((F.length(F.col(prep_time)) == 4) &
                                                        (F.substring(F.col(prep_time), 4, 1) == 'M'),
                                              (F.substring(F.col(prep_time), 3, 1)).cast('int'))
                           .when((F.length(F.col(prep_time)) == 4) & (F.substring(F.col(prep_time), 4, 1) == 'H'),
                                              ((F.substring(F.col(prep_time), 3, 1)).cast('int')) * 60)
                           .when((F.length(F.col(prep_time)) == 5), (F.substring(F.col(prep_time), 3, 2)).cast('int'))
                           .when((F.length(F.col(prep_time)) == 6), (((F.substring(F.col(prep_time), 3, 1)).cast('int')) * 60) +
                                 ((F.substring(F.col(prep_time), 5, 1)).cast('int')))
                           .when((F.length(F.col(prep_time)) == 7), (((F.substring(F.col(prep_time), 3, 1)).cast('int')) * 60) +
                                 ((F.substring(F.col(prep_time), 5, 2)).cast('int'))).otherwise(F.lit(0))
                           )

    return df_to_be_formatted


@udf
def duration_in_proper_format(difficulty, avg_total_cooking_time):
    '''
    Converting the Duration of each difficulty type to readable format
    :param difficulty:
    :param avg_total_cooking_time:
    :return: Dataframe with Formated duration
    '''
    #Check if difficulty is hard, then convert duration in Hours and minutes form
    if difficulty == 'hard':
        rounded_duration = int(round(avg_total_cooking_time))
        hours = int(rounded_duration / 60)
        minutes = rounded_duration % 60
        return f"{hours} hours & {minutes} minutes"
    else:
        #If difficulty is easy or medium, keep it in minutes
        rounded_duration = int(round(avg_total_cooking_time))
        return f"{rounded_duration} minutes"


def filename_change(output_path,output_file_name):
    '''
    Renames the output file to the required name
    :param output_path:
    '''
    os.chdir(output_path) #Going tto the folder where we need to change the name
    #Iterating through each file name to check if it has '.csv' as its extension
    for file in glob.glob("*.csv"):
        filename = file
        break

    new_path = output_path + filename   #Creating path of the file to be renamed
    rename_path = output_path + output_file_name #Creating the new name of the file with which we want to rename

    #This renames the file to our specified name
    os.rename(new_path, rename_path)


def  get_recipes_involving_beef(input_reciepie_df):
    '''
    Filter out only those recipes with Beef in the ingredients
    :param input_reciepie_df:
    :return: only_beef_in_ingredients_df
    '''

    # Selecting only the columns which are required for computations and dropping others
    input_reciepie_df = input_reciepie_df.select("name", "ingredients", "prepTime", "cookTime")

    # Changing the case of the string so that it becomes easier for comparision
    is_beef_in_ingredients_df = input_reciepie_df.withColumn("ingredients_to_upper",
                                                             F.upper(input_reciepie_df['ingredients']))

    # Checking if 'BEEF' is there in the ingredients
    is_beef_in_ingredients_df = is_beef_in_ingredients_df.withColumn("contains_beef",
                                                                     F.col('ingredients_to_upper').contains('BEEF'))


    # Filtering records with BEEF in the ingredients and drop the columns which are not required
    only_beef_in_ingredients_df = is_beef_in_ingredients_df.filter(F.col('contains_beef') == 'true') \
        .drop('ingredients', 'ingredients_to_upper','contains_beef')

    return only_beef_in_ingredients_df


def calculate_total_cooking_time(only_beef_in_ingredients_df):
    '''
    Calcultes total Cooking time by adding prep time and cook time for each recipe
    :param only_beef_in_ingredients_df:
    :return: total_cook_time_df
    '''

    # Getting Preperation Time in proper format so that we can perform mathematical operations over it
    prep_time_formated_df = get_minutes(only_beef_in_ingredients_df,'prepTime')

    # Getting Cooking Time in proper format so that we can perform mathematical operations over it
    cook_time_formatted_df =  get_minutes(prep_time_formated_df,'cookTime')

    # Calculating  total cooking time by adding Cook Time and Prep time
    total_cook_time_df = cook_time_formatted_df.withColumn('total_cook_time', (
            F.col('prepTime') + F.col('cookTime')).cast('int'))

    # Dropping columns which are not required
    total_cook_time_df = total_cook_time_df.drop("prepTime", "cookTime", "name",'contains_beef')

    return total_cook_time_df


def calculate_average_cooking_time_per_difficulty_level(total_cook_time_df):
    '''
    Gives difficulty level according to their individual cooking time and
    Calclates average cooking time for every difficulty level
    :param total_cook_time_df:
    :return: avg_cooking_time_df
    '''
    # Classifying the recipes the basis of their difficulty levels
    difficulty_level_classified_df = total_cook_time_df.withColumn("difficulty",
                                                                   when(F.col('total_cook_time') <= 30, 'easy')
                                                                   .when(F.col('total_cook_time') <= 60, 'medium')
                                                                   .otherwise('hard'))

    # Calculating average duration per difficulty level
    difficulty_level_classified_df = difficulty_level_classified_df.groupby('difficulty').mean()

    #REduce the partitions to one as we have only 3 rows
    difficulty_level_classified_df.coalesce(1)

    # Ordering as easy, medium and hard
    difficulty_level_classified_df = difficulty_level_classified_df.orderBy('avg(total_cook_time)')

    # Changing the duration in a proper readable and presentable format
    avg_cooking_time_df = difficulty_level_classified_df.withColumn("avg_total_cooking_time",
                                                                    duration_in_proper_format(F.col('difficulty'),
                                                                    F.col('avg(total_cook_time)')))

    # dropping the column which is not required
    avg_cooking_time_df = avg_cooking_time_df.drop('avg(total_cook_time)')

    return avg_cooking_time_df


def main():
    global logger

    #Initializing Configs and Paths
    config = configparser.ConfigParser()
    config.read('config.ini')
    input_path_S3 = config.get('INPUT_PATH', 'input_path')
    output_path = config.get('OUTPUT_PATH', 'output_path')
    output_file_name = config.get('OUTPUT_PATH', 'output_file_name')
    log_path = config.get('LOGS', 'log_path')
    environment = config.get('ENVIRONMENT', 'environment')

    #Initializing Logger
    logger = util.init_logger('AvgDurationPerDifficultyLevel.py',log_path)

    #Initializing SparkSession
    if environment == 'local':
        spark = SparkSession.builder.master('local[*]').appName('HelloFreshAssigment').getOrCreate()
    else:
        spark = SparkSession.builder.appName('HelloFreshAssigment').getOrCreate()

    logger.info("Spark Session Initialised")

    try:
        #Reading File From Specified Path
        input_recipe_df = spark.read.json(input_path_S3)
        logger.info(f"File got read successfully from the loation {input_path_S3}")
    except:
        logger.error(f"There is some issue with the file input path: {input_path_S3}")

    #Calling function to get records which only contain ingredient beef
    only_beef_in_ingredients_df = get_recipes_involving_beef(input_recipe_df)

    #Calculating Total cooking time for each recipe
    total_cook_time_df = calculate_total_cooking_time(only_beef_in_ingredients_df)

    #Calculating Average Cooking time per difficulty level
    avg_cooking_time_df = calculate_average_cooking_time_per_difficulty_level(total_cook_time_df)

    try:
        #Writing data to the specified output path and overwriting if it already exists
        avg_cooking_time_df.coalesce(1)\
            .write.mode('overwrite')\
            .format("csv") \
            .option("header", "true") \
            .save(output_path)
        logger.info(f"File got written successfully at the loation {output_path}")
    except:
        logger.error(f"There is some issue with the file input path: {output_path}")

    #Stopping the Spark Session so that resources are released
    spark.stop()

    #As the file written by spark has a name something like - 'part000-*.csv'
    #We need to change it to reports.csv as per the deliverable requirement
    try:
        filename_change(output_path,output_file_name)
        logger.info("Filname changed Successfully")
    except:
        logger.error(f"There is some issue with {output_path} or {output_file_name}")


#Calling the main()
if __name__ == '__main__':
    main()
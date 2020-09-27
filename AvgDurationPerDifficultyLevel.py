from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when
from pyspark.sql import functions as F
import os, glob
import configparser
import util

@udf
def ingredients_splitting(ingredients):
    flag = 0
    split_ingredients = ingredients.split('\n')

    for single_ingredient in split_ingredients:
        word_in_ingredient = single_ingredient.strip().split(' ')
        for is_beef in word_in_ingredient:
            if is_beef.strip("(").strip(")").strip(",").strip("-").strip(".").lower() == 'beef':
                flag = 1
                break

    if flag == 1:
        return 1
    else:
        return 0


@udf
def get_minutes(prep_time):
    split_time = ''

    if len(prep_time) == 2:
        return 0

    elif len(prep_time) == 4:
        split_time = prep_time[2:]
        if split_time[-1] == 'H':
            return int(split_time[0]) * 60
        else:
            return int(split_time[0])

    elif len(prep_time) == 5:
        split_time = prep_time[2:]
        return int(split_time[0:2])

    elif len(prep_time) == 6:
        split_time = prep_time[2:]
        return (int(split_time[0]) * 60) + (int(split_time[2]))

    elif len(prep_time) == 7:
        split_time = prep_time[2:]
        return (int(split_time[0]) * 60) + (int(split_time[2:4]))

    else:
        return 0


@udf
def duration_in_proper_format(difficulty, avg_total_cooking_time):
    if difficulty == 'hard':
        rounded_duration = int(round(avg_total_cooking_time))
        hours = int(rounded_duration / 60)
        minutes = rounded_duration % 60
        return f"{hours} hours & {minutes} minutes"
    else:
        rounded_duration = int(round(avg_total_cooking_time))
        return f"{rounded_duration} minutes"


def filename_chnge(output_path):
    os.chdir(output_path)
    for file in glob.glob("*.csv"):
        filename = file
        break

    new_path = output_path + filename
    rename_path = output_path + 'reports.csv'
    os.rename(new_path, rename_path)


def main():
    global logger
    config = configparser.ConfigParser()
    config.read('config.ini')
    input_path = config.get('LOCATIONS', 'input_path')
    output_path = config.get('LOCATIONS', 'output_path')
    log_path = config.get('LOCATIONS', 'log_path')
    logger = util.init_logger('AvgDurationPerDifficultyLevel.py',log_path)

    spark = SparkSession.builder.master('local[*]').appName('HelloFreshAssigment').getOrCreate()

    input_reciepie_df = spark.read.json(input_path)

    #logger.info(f'Input Path :{input_path}')


    input_reciepie_df = input_reciepie_df.select("name", "ingredients", "prepTime", "cookTime")

    is_beef_in_ingredients_df = input_reciepie_df.withColumn("ingredients_to_upper", F.upper(input_reciepie_df['ingredients']))

    is_beef_in_ingredients_df = is_beef_in_ingredients_df.withColumn("contains_beef", is_beef_in_ingredients_df['ingredients_to_upper'].contains('BEEF'))

    is_beef_in_ingredients_df.select('contains_beef','ingredients_to_upper').show(50,False)
    #is_beef_in_ingredients_df = input_reciepie_df.withColumn("is_beef_true", ingredients_splitting(
      #  input_reciepie_df['ingredients']))

    only_beef_in_ingredients_df = is_beef_in_ingredients_df.filter(
        is_beef_in_ingredients_df.is_beef_true == 1).drop('ingredients')

    prep_time_formated_df = only_beef_in_ingredients_df.withColumn("prep_time_in_minutes", get_minutes(
        only_beef_in_ingredients_df['prepTime']))

    cook_time_formatted_df = prep_time_formated_df.withColumn("cook_time_in_minutes", get_minutes(
        prep_time_formated_df['cookTime']))

    cook_time_formatted_df = cook_time_formatted_df.drop('prepTime', 'cookTime', 'is_beef_true')

    total_cook_time_df = cook_time_formatted_df.withColumn('total_cook_time', (
            cook_time_formatted_df['prep_time_in_minutes'] + cook_time_formatted_df['cook_time_in_minutes']).cast(
        'int'))

    total_cook_time_df = total_cook_time_df.drop("prep_time_in_minutes", "cook_time_in_minutes",
                                                 "name")
    difficulty_level_classified_df = total_cook_time_df.withColumn \
        ("difficulty", when(total_cook_time_df['total_cook_time'] <= 30, 'easy')
         .when(total_cook_time_df['total_cook_time'] <= 60, 'medium')
         .otherwise('hard'))

    difficulty_level_classified_df = difficulty_level_classified_df.groupby('difficulty').mean()

    difficulty_level_classified_df = difficulty_level_classified_df.orderBy('avg(total_cook_time)')

    avg_cooking_time_df = difficulty_level_classified_df.withColumn("avg_total_cooking_time",
                                                                    duration_in_proper_format(
                                                                        difficulty_level_classified_df['difficulty'],
                                                                        difficulty_level_classified_df[
                                                                            'avg(total_cook_time)']))

    avg_cooking_time_df = avg_cooking_time_df.drop('avg(total_cook_time)')

    spark.stop()


if __name__ == '__main__':
    main()
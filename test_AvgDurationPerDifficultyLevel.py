import unittest
from unittest import TestCase
from AvgDurationPerDifficultyLevel import get_recipies_involving_beef, calculate_total_cooking_time, \
    calculate_average_cooking_time_per_difficulty_level
from pyspark.sql import SparkSession
import util

# Initializing Configs and Paths
input_path = "C://Users/Rishi/Desktop/HelloFreshAssignment/sample.json"
log_path = "C://Users/Rishi/Desktop/HelloFreshAssignment/logs/test_logs"

# Initializing Logger
logger = util.init_logger('test_AvgDurationPerDifficultyLevel.py', log_path)

# Initializing SparkSession
spark = SparkSession.builder.master('local[*]').appName('HelloFreshAssigment').getOrCreate()

logger.info("Spark Session Initialised")

try:
    # Reading File From S3
    input_reciepie_df = spark.read.json(input_path)
    logger.info(f"File got read successfully from the location {input_path}")
except:
    logger.error(f"There is some issue with the file input path: {input_path}")


# Calling functions to respective values in different stages
only_beef_in_ingredients_df = get_recipies_involving_beef(input_reciepie_df)
total_cook_time_df = calculate_total_cooking_time(only_beef_in_ingredients_df)
avg_cooking_time_df = calculate_average_cooking_time_per_difficulty_level(total_cook_time_df)



class Test(TestCase):
    # Calling functions to respective values in different stages

    def test_get_recipies_involving_beef(self):
        expected_beef_occurences = spark.createDataFrame(data=[['abc', 'PT5M', 'PT20M'],
                                                               ['def', 'PT20M', 'PT3H'],
                                                               ['ijk', 'PT45M', 'PT']],
                                                         schema=['name', 'prepTime', 'cookTime'])

        self.assert_(expected_beef_occurences, only_beef_in_ingredients_df)

    def test_calculate_total_cooking_time(self):
        expected_cooking_time = spark.createDataFrame(data=[[25],
                                                            [200],
                                                            [45]],
                                                      schema=['total_cook_time'])

        self.assert_(expected_cooking_time, total_cook_time_df)

    def test_calculate_average_cooking_time_per_difficulty_level(self):
        expected_avg_cooking_time = spark.createDataFrame(data=[['easy', '25 minutes'],
                                                                ['medium', '45 minutes'],
                                                                ['hard', '3 hours & 20 minutes']],
                                                          schema=['difficulty', 'avg_total_cooking_time'])

        self.assert_(expected_avg_cooking_time, total_cook_time_df)


if __name__ == '__main__':
    unittest.main()

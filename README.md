# HelloFresh Average Cooking Duration Per Difficulty Level

## Problem Statement 
Given a .json file with a collection of recipes stored on S3, need to:

* Extract only recipes that have beef as one of the ingredients
* Calculate average cooking time duration per difficulty level
* Total cooking time duration can be calculated by formula:

```total_cook_time = cookTime + prepTime```

Criteria for levels based on total cook time duration:
* **easy** - less than 30 minutes
* **medium** - between 30 and 60 minutes
* **hard** - more than 60 minutes.

## Deliverables
* A deployable Spark Application written in Python
* A README file with brief explanation of approach, data exploration and assumptions/considerations. You can use this file by adding new section or create a new one.
* A CSV file with average cooking time per difficulty level. Please add it to output folder. File should have 2 columns: difficulty,avg_total_cooking_time and named as report.csv

## Data Exploration
 Data has following nine columns:
* name
* ingredients
* url
* image
* cookTime
* recipeYield
* datePublished
* prepTime
* description

- Out of the above mentioned columns only four columns namely **name, ingredients, prepTime and cookTime** are important to us. So we will drop other columns initially and select only the required columns
- *prepTime* and *cookTime* are not in desired format, so we need to convert them using an UDF. eg. We need to convert given prep time as 'PT50M' to 50 so that we can perform mathematical operations over it.
- *ingredients* contains a long description of different ingredients with their respective quantity separated by '\n'. We just need to check if in the list of ingredients, if beef is there or not. We will filter only those columns who pass this check.

## Assumptions and Considerations
- I have developed and executed the code on my local as I don't have any Spark cluster environment access for my personal work. Code is tested on local as well But it is scalable and can adapt to higher requirements on the go.
- If *prepTime* is 'PT' that means it is zero.
- *prepTime* or *cookTime* individually won't be more than 10 hours
- *prepTime* or *cookTime* won't be null
- Just kept the *name* column and did not drop it because data is distinguishable in initial stages
- Need final output in some readable format like *2 hours and 10 minutes* rather than just *130* 
- We need to overwrite the *reports.csv* in every run because the value in the *Recipe Repository* keeps getting updated periodically
- **0-30** minutes is *easy*
- **31-60** minutes is *medium*
- **61-onwards** minutes is *hard*
- Rounded off the averages of the respective difficulty levels to nearest integer value

## Explanation of Approach
- Read the .json and stored it in a dataframe 
- First I dropped the columns which were not required and kept only those which were needed for the calculations
- I then checked every row in *ingredients* column if *beef* was there present in it or not. If yes then it would add *true* to *contains_beef* column otherwise would add *false*
- Next step was to filter the data with only true values and again dropping all the irrelevant columns
- Then I converted the *prepTime* and *cookTime* in a proper integer format using pyspark.sql functions
- Then added *prepTime* and *cookTime* to get *total_cook_time* for each recipe
- After this I assigned difficulty levels to each recipe according to their respective *total_cook_time*
- Then I calculated average of every *difficulty* level and did a *groupBy* over the *difficulty* level.
- Then I did final rearrangements for data representation so that it looks in a presentable format and called an UDF for it
- Then I written it to the specified folder and did a *coalesce(1)* so that only one file is written
- I will stop spark session so that the resources are released
- As the file written by spark has a different name than what we want, I will use python code to rename it to the desired name.

## Performance Tuning
- Dropped columns as soon as not required
- Filtering the recipes only with ingredient as beef
- Tried to perform maxiumum opertaions/functions using spark inbuilt functions rather than using udf
- Stopping the Spark Session as soon as the Spark related tasks are done
- Did a coalesce(1) as soon as we had only 3 rows with us
- Took all the paths,names from config.ini so that we can pass as and when requirement changes on the go

## CI/CD Explanation
- Wrote unit test cases which covers every function in the code (*test_AvgDurationPerDifficultyLevel.py*)
- If deployed and the test cases fail, the build will automatically fail
- Tried covering maximum code in unit tests to increase overall code coverage

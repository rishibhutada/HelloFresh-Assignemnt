# HelloFresh Average Cooking Duration Per Difficulty Level

## Problem Statement 
Given a .json file a collection of recipes stored on S3, need to find:
Using Apache Spark and Python read processed dataset from S3

*Extract only recipes that have beef as one of the ingredients
*Calculate average cooking time duration per difficulty level
*Total cooking time duration can be calculated by formula:
```total_cook_time = cookTime + prepTime```
*Criteria for levels based on total cook time duration:
**easy** - less than 30 mins
**medium** - between 30 and 60 mins
**hard** - more than 60 mins.

## Deliverables
* A deployable Spark Application written in Python
* A README file with brief explanation of approach, data exploration and assumptions/considerations. You can use this file by adding new section or create a new one.
* A CSV file with average cooking time per difficulty level. Please add it to output folder. File should have 2 columns: difficulty,avg_total_cooking_time and named as report.csv

## Solution 
-Following is the approach that I have taken for the solution

##Data Exploration
- Data has following nine columns:
*name
*ingredients
*url
*image
*cookTime
*recipeYield
*datePublished
*prepTime
*description

- Out of the above mentioned columns only four columns namely **name,ingredients,prepTime and cookTime** are important to us. So we will drop other columns initially and select only the required columns
- *prepTime* and *cookTime* are not in desired format, so we need to convert them using an UDF. eg. We need to convert given prep time as 'PT50M' to 50 so that we can perform mathematical operations over it.
- *ingredients* contains a long description of different ingredients with their respective quantity separated by '\n'. We just need to check if in the list of ingredients, if beef is there or not. We will filter only those columns who pass this check.



## License
[MIT](https://choosealicense.com/licenses/mit/)
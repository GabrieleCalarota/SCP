# Amazon Food Reviews

## Description

This project allows the analysis of amazon food review dataset, which can be found here:

- [Stanford dataset](https://snap.stanford.edu/data/web-FineFoods.html)

The dataset and all the other resources will be downloaded automatically as the project is executed.


## Build & Execution

### Setup AWS
 - Create a bucket in S3 Storage
 - Upload all the project files (Only the test files are required)
 - Compile the project and upload the jar file to the bucket
 - Setup a cluster (EMR). The project has been tested under this configurations: emr-5.30.1, m5.xlarge, Spark 2.4.5 - Zeppelin 0.8.2
 - Connect through ssh to the master node
 ```
 Example: ssh -i ~/prova.pem hadoop@ec2-34-205-140-244.compute-1.amazonaws.com
 ```
 - Copy the jar file from the s3 bucket to the master node
 ```
 Example: aws s3 cp s3://foodreview/FoodReview.jar .
 ```

### Execution

To compile the project and create the jar file

```
$ cd FoodReviewFinal
$ sbt assembly
```

- To execute it locally 
    ```
    $ export environment=local
    $ spark-submit FoodReview.jar
    ```

- To execute it on aws (*DEFAULT OPTION*)
    ```
    export path="path-to-s3-location-goes-here"
    $ spark-submit FoodReview.jar
    ```

### Test provided

In `src/test/scala` we provided a set of tests, one for each operation allowed with the dataset provided and a `totalTest.txt` with a mixin of operation.

Running the project, the default file loaded for test is `totalTest.txt`. To prevent the default behaviour, set the environment variable like below:

```$xslt
$ export testFileName=testRecommendation.txt
```

## Analysis

### Product Recommendation

Computes user recommended products, considering its previous reviews and ratings compared to similar users.

**Note**: If no userID is given in input, the program picks one random userID from the dataset 

```
recommend (userID)
```

### Product Ranking

Computes the product ranking with the bayesian mean rating algorithm.

```
rank
```

### Product Time Analysis

Creates a csv about the evolution of rating average in time, with columns 
```
| productId | year/month | avgRating |
```

There are two type of analysis: analysis in an interval of years and analysis in a given year by month.
The csv file are stored in "resources" folder, with schema: `P year/month _ productID _ YEAR(S) _ YYYY (_YYYY).csv`

*Example: `PY_B001BDDTB2_YEARS_2009_2012.csv` = Product time analysis, yearly based for productID
 B001BDDTB2 between 2009 and 2012*

#### Analysis in an interval of years

```
evolutionY yearBegin yearEnd productID(s)
```

**Note:** to not provide the year of begin (end), replace them with a '-'. 
In this case we will consider the first(last) year recorded into the dataset.

#### Analysis per month in a specific year

```
evolutionM year productID(s)
```

### User Helpfulness

Computes the helpfulness of users' reviews rank.

**Note**: 
 - If an user has an helpfulness score that is lower than the average (of the other users that gave the same rating to the same product), 
its score is incremented by adding the average score to the initial score and dividing by 2. 
The final helpfulness score is the average of the user helpfulness for the evaluated products.

*Optional arguments:*
 - threshold: filter of helpfulness score for users greater than this value (*default 0*)
 - limit: limit query by this number of rows (*default 20*)

```
helpfulness (userID) (threshold) (limit)
```

© Gabriele Calarota, Alberto Drusiani.

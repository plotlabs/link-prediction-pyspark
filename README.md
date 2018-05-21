# link-prediction-pyspark
A Pyspark implementation of the [CNGF Algorithm](https://www.hindawi.com/journals/mpe/2013/125123/) used for Link Prediction.  
  
**CNGF Algorithm**  
The [algorithm](https://www.hindawi.com/journals/mpe/2013/125123/alg1/) helps in predicting which nodes in a graph are most likely to be connected in the future. This can be used for Social Networks to envision connection between various entities.  
  
The algorithm proves to be more efficient than traditional algorithms as it uses the subgraph of two nodes x and y and their common neighbours to forsee their connection in future and not the whole graph. It first calculates the *Guidance* by dividing the degree of a common neighbour in the subgraph with the log of degree of that neighbour in the whole graph. Then it takes the sum of guidances of all common neighbours of x and y to compute *Similarity*. Higher the similarity, more the chance of a connection in future.  
## Requires   

 1. Python 2.7+
 2. [Apache Spark 2.0.0+](http://spark.apache.org/downloads.html)
 3. [Graphframes 0.2.0+](https://spark-packages.org/package/graphframes/graphframes)
 
## Usage
To run the program, clone the repository and run the following command:

    $SPARK_HOME/bin/spark-submit --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 cngf.py file_path separator

It requires 2 arguments:

 - file_path: path of the file containing the data.
 - separator: the column separator used in the file

## Example
As an example, the program can be run on the example txt file - **example.txt**. The columns in this file are separated using **space**, so to run the program on this file, run the following command:

    $SPARK_HOME/bin/spark-submit --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 cngf.py "example.txt" " "

## Reference
Liyan Dong, Yongli Li, Han Yin, Huang Le, and Mao Rui, “The Algorithm of Link Prediction on Social Network,” _Mathematical Problems in Engineering_, vol. 2013, Article ID 125123, 7 pages, 2013. [https://doi.org/10.1155/2013/125123](https://doi.org/10.1155/2013/125123).
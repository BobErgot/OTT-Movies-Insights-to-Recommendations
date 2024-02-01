#  OTT Movies: From Insights to Recommendations

## Introduction
This project utilizes MapReduce to perform a comprehensive analysis of movie ratings and to build a movie recommendation system. By leveraging MapReduce's distributed data processing, we can efficiently handle large datasets, such as the Netflix prize data, to compute various statistical functions and generate personalized movie recommendations.

## Movie Recommendation System
### Overview
The recommendation system employs the Apriori frequent itemset mining algorithm within a MapReduce framework to uncover relationships between movies based on user ratings. By identifying sets of movies that frequently appear together in user rating patterns, we can recommend new movies to a user that they are likely to enjoy.

### Implementation with MapReduce
The recommendation system is implemented through several MapReduce jobs that analyze the entire dataset to discover frequent movie sets and generate recommendations based on these sets. The process includes:

1. **Data Preprocessing**: Initial job filters and prepares the data by extracting relevant fields necessary for the Apriori algorithm, ensuring the data meets quality and formatting standards required for effective mining.

2. **Frequent Itemset Generation**:
	- **First Pass**: Identifies frequent individual movies across all user ratings by counting occurrences and comparing them to a predefined support threshold.
	- **Subsequent Passes**: Uses the results from the first job to find frequent pairs, triples, etc., by combining itemsets from the previous iteration and checking their frequency against the support threshold. This process iterates until no new frequent itemsets are found or a maximum iteration limit is reached.

3. **Rule Generation and Filtering**:
	- After identifying frequent itemsets, another set of jobs generates association rules based on these itemsets (e.g., if a user likes both Movie A and Movie B, they might also like Movie C).
	- Rules are filtered based on confidence levels to ensure that only the most probable recommendations are made.

4. **Recommendation Compilation**:
	- The final job compiles recommendations for each user based on the association rules and the user’s existing ratings.
	- Outputs a list of movie recommendations for each user, sorted by the likelihood of interest as determined by the confidence levels of the applicable rules.

### Benefits of Apriori in Recommendations
Utilizing Apriori for movie recommendations offers several advantages:
- **Scalability**: Efficiently handles large datasets by systematically reducing itemset candidates in successive passes.
- **Accuracy**: By focusing on the most frequent and significant correlations between movies, recommendations are more likely to resonate with user preferences.
- **Interpretability**: The association rules generated from frequent itemsets provide clear insights into why certain movies are recommended, enhancing transparency.

## Dataset
The dataset for this project is sourced from the [Netflix Prize data available on Kaggle](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data/data). This dataset is structured for analysis with an updated format and is essential for building our movie recommendation system.

### Dataset Structure
- **Data Points:** The dataset contains over 100 million movie ratings.
- **Users:** Ratings were provided by nearly 480,000 anonymized, randomly-chosen Netflix subscribers.
- **Movies:** The dataset covers over 17,000 movies.
- **Rating Scale:** Ratings are on a scale from 1 to 5 stars.
- **Time Frame:** The dataset includes movie ratings from October 1998 to December 2005.

The training dataset is structured as follows, and we have tailored our processing to fit this format efficiently:
```
    movie_id:
    user_id, rating, date
    user_id2, rating, date
```


Each `movie_id` acts as a header followed by user ratings, which include `user_id`, `rating`, and `date` of the rating.

### Data Processing
We utilize a Python script, `input_data_processor.py`, to reformat the raw data into a more manageable form. The script processes the original files from the dataset to segregate data into distinct blocks for each movie, facilitating easier data manipulation and analysis.

#### Script Overview
The `input_data_processor.py` script performs the following operations:
- It scans the specified input directory for files matching the pattern 'combined_data_*.txt', which are the original dataset files.
- For each file, it inserts additional newline characters before each movie ID to separate the data clearly.
- The processed data is saved into new files within a designated output directory, maintaining the original file names for consistency.

#### How to Use the Script
To use the script, follow these steps:
1. Ensure Python is installed on your system.
2. Place the script in a directory accessible to your dataset.
3. Run the script with the input directory (where the dataset files are located) and the output directory (where you want the processed files to be saved) as arguments.

```bash
python input_data_processor.py /path/to/input/directory /path/to/output/directory
````

Installation
------------
These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

Environment
-----------
1) Example ~/.bash_aliases:
   ```
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_HOME=/usr/local/hadoop-3.3.5
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   ```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

   `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
   Sufficient for standalone: hadoop.root, jar.name, local.input
   Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws`					-- check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination

Author
-----------
- [Bobby Doshi](https://github.com/BobErgot)
- [Anant Moudgalya](https://github.com/anantmoudgalya)
- [Apurva Khatri](https://github.com/apurvakhatri)

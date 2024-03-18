package apriori;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URI;

/**
 * Implements MapReduce application to perform frequent itemset mining for movie datasets using the Apriori algorithm.
 * This class manages the execution of multiple MapReduce jobs that filter data, calculate support, and generate association rules.
 * It also integrates with AWS S3 for input and output operations when not running locally.
 */
public class AprioriFrequentMovieSet extends Configured implements Tool {
  private static final Logger logger = Logger.getLogger(AprioriFrequentMovieSet.class);
  private static int MAX_ITERATION;
  private static int MAX_MOVIE_ID;
  private static int MAX_USER_ID;
  private static double SUPPORT_THRESHOLD;
  private static String BUCKET_NAME_STRING;
  private static String BUCKET_NAME;
  private static boolean IS_LOCAL;

  /**
   * Entry point for the AprioriFrequentMovieSet MapReduce application.
   * Parses command line arguments and initiates the MapReduce process.
   *
   * @param args Command line arguments that include input and output paths, maximum iterations, ID thresholds, support threshold, bucket name, and local flag.
   * @throws Exception if there is an issue executing the MapReduce jobs.
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AprioriFrequentMovieSet(), args);
    System.exit(exitCode);
  }

  /**
   * Configures and executes the sequence of MapReduce jobs necessary for the Apriori frequent itemset mining algorithm.
   * Validates and sets up input/output paths, parameters for job configuration, and controls the flow of job execution.
   *
   * @param args An array of command-line arguments specifying job parameters and paths.
   * @return an integer status code representing the success or failure of the job execution.
   * @throws Exception if an error occurs during the job configuration or execution.
   */
  @Override
  public int run(final String[] args) throws Exception {
    if (args.length != 8) {
      System.err.println("Usage: AprioriFrequentMovieSet <input path> <output path> <max iterations> <max movie id> " + "<max user id> <support threshold> <bucket name> <local flag>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    // Parsing command line arguments
    String inputDir = args[0];
    String outputDir = args[1];
    MAX_ITERATION = Integer.parseInt(args[2]);
    MAX_MOVIE_ID = Integer.parseInt(args[3]);
    MAX_USER_ID = Integer.parseInt(args[4]);
    SUPPORT_THRESHOLD = Double.parseDouble(args[5]);
    BUCKET_NAME_STRING = args[6];
    BUCKET_NAME = "s3://" + BUCKET_NAME_STRING;
    IS_LOCAL = Boolean.parseBoolean(args[7]);

    if (!IS_LOCAL) {
      inputDir = BUCKET_NAME + "/" + inputDir;
      outputDir = BUCKET_NAME + "/" + outputDir;
    }

    // Initialize and run each job
    int userCount = setupAndRunJob1(inputDir, outputDir + "/iteration_0");
    if (userCount <= 0) {
      logger.error("Job 1 did not complete successfully.");
      return 1;
    }

    Counter movieSetCounter = setupAndRunJob2(inputDir, outputDir + "/iteration_1", userCount);
    if (movieSetCounter == null) {
      logger.error("Job 2 did not complete successfully.");
      return 1;
    }

    int iteration = 2;
    do {
      boolean success = setupAndRunJob3(outputDir, outputDir, iteration);
      if (!success) {
        logger.error("Job 3 did not complete successfully.");
        return 1;
      }

      Job job4 = Job.getInstance(getConf(), "Association Rule Mining Iteration " + iteration);
      job4.setJarByClass(AprioriFrequentMovieSet.class);
      configureJob4(job4, outputDir, outputDir, userCount, iteration);
      success = job4.waitForCompletion(true);
      if (!success) {
        logger.error("Job 4 did not complete successfully.");
        return 1;
      }

      movieSetCounter = job4.getCounters().findCounter(Constants.Counters.MOVIE_SET_COUNT);
      iteration++;
    } while (movieSetCounter.getValue() > 1 && iteration <= MAX_ITERATION);

    return 0;
  }

  private int setupAndRunJob1(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance(getConf(), "User Rating Threshold Filter");
    job.setJarByClass(AprioriFrequentMovieSet.class);
    configureJob1(job, inputPath, outputPath);
    boolean completion = job.waitForCompletion(true);
    return completion ? (int) job.getCounters().findCounter(Constants.Counters.USER_COUNT).getValue() : -1;
  }

  private Counter setupAndRunJob2(String inputPath, String outputPath, int userCount) throws Exception {
    Job job = Job.getInstance(getConf(), "Calculate Movie List Support and Aggregate");
    job.setJarByClass(AprioriFrequentMovieSet.class);
    configureJob2(job, inputPath, outputPath, userCount);
    boolean completion = job.waitForCompletion(true);
    return completion ? job.getCounters().findCounter(Constants.Counters.MOVIE_SET_COUNT) : null;
  }

  private boolean setupAndRunJob3(String inputPath, String outputPath, int iteration) throws Exception {
    Job job = Job.getInstance(getConf(), "Candidate Movie Set Generation: Iteration " + iteration);
    job.setJarByClass(AprioriFrequentMovieSet.class);
    configureJob3(job, inputPath, outputPath, iteration);
    return job.waitForCompletion(true);
  }

  private void configureJob1(Job job, String inputPath, String outputPath) throws Exception {
    Configuration jobConf = job.getConfiguration();
    jobConf.set("textinputformat.record.delimiter", "\n\n");
    jobConf.setInt(Constants.MAX_USER_ID, MAX_USER_ID);
    jobConf.setInt(Constants.MAX_MOVIE_ID, MAX_MOVIE_ID);

    job.setMapperClass(DataPreprocessMapper.class);
    job.setReducerClass(UserCounterReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
  }

  private void configureJob2(Job job, String inputPath, String outputPath, int userCount) throws Exception {
    Configuration jobConf = job.getConfiguration();
    jobConf.set("textinputformat.record.delimiter", "\n\n");
    jobConf.setDouble(Constants.SUPPORT_THRESHOLD, SUPPORT_THRESHOLD);
    jobConf.setInt(Constants.TOTAL_USERS, userCount);
    jobConf.setBoolean(Constants.IS_LOCAL, IS_LOCAL);
    jobConf.set(Constants.BUCKET_NAME, BUCKET_NAME);
    jobConf.setInt(Constants.CURRENT_ITERATION, 1);
    jobConf.setInt(Constants.MAX_USER_ID, MAX_USER_ID);
    jobConf.setInt(Constants.MAX_MOVIE_ID, MAX_MOVIE_ID);

    job.setMapperClass(MovieSupportFilterMapper.class);
    job.setReducerClass(UserMovieAggregationReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    MultipleOutputs.addNamedOutput(job, Constants.MOVIES_DIR, TextOutputFormat.class, LongWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, Constants.USERS_DIR, TextOutputFormat.class, LongWritable.class, Text.class);
  }

  private void configureJob3(Job job, String inputPath, String outputPath, int iteration) throws Exception {
    Configuration jobConf = job.getConfiguration();
    jobConf.setBoolean(Constants.IS_LOCAL, IS_LOCAL);
    jobConf.set(Constants.BUCKET_NAME, BUCKET_NAME);
    jobConf.setInt(Constants.CURRENT_ITERATION, iteration);

    job.setMapperClass(CandidateSetGeneratorMapper.class);
    job.setReducerClass(MovieSetFinalizerReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Determine input and output paths based on whether the operation is local or S3
    String interPath = outputPath + Constants.FILE_SEPARATOR + Constants.ITERATION_PREFIX + (iteration - 1) + Constants.FILE_SEPARATOR + Constants.MOVIE_SETS_PATH;
    TextInputFormat.addInputPaths(job, interPath);
    FileOutputFormat.setOutputPath(job, new Path(outputPath + Constants.FILE_SEPARATOR + Constants.JOIN_ITERATION_PREFIX + iteration));

    // Handle cache files
    if (IS_LOCAL) {
      File dir = new File(interPath);
      if (dir.exists()) {
        for (String path : dir.list()) {
          if (!path.endsWith(Constants.CRC_EXTENSION) && !path.startsWith(Constants.SUCCESS_FILE)) {
            job.addCacheFile(new URI(interPath + Constants.FILE_SEPARATOR + path));
          }
        }
      }
    } else {
      final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME_STRING).withPrefix(interPath);
      ObjectListing objects = s3Client.listObjects(listObjectsRequest);
      for (S3ObjectSummary summary : objects.getObjectSummaries()) {
        String path = summary.getKey();
        if (!path.endsWith(Constants.CRC_EXTENSION) && !path.startsWith(Constants.SUCCESS_FILE)) {
          logger.info("Added File " + BUCKET_NAME + Constants.FILE_SEPARATOR + path);
          job.addCacheFile(new URI(BUCKET_NAME + Constants.FILE_SEPARATOR + path));
        }
      }
    }

    job.getCacheFiles();
  }

  private void configureJob4(Job job, String inputPath, String outputPath, int userCount, int iteration) throws Exception {
    Configuration jobConf = job.getConfiguration();
    jobConf.setInt(Constants.CURRENT_ITERATION, iteration); // This should be dynamic based on the actual iteration
    jobConf.setDouble(Constants.SUPPORT_THRESHOLD, SUPPORT_THRESHOLD);
    jobConf.setInt(Constants.TOTAL_USERS, userCount);
    jobConf.setBoolean(Constants.IS_LOCAL, IS_LOCAL);
    jobConf.set(Constants.BUCKET_NAME, BUCKET_NAME);

    job.setMapperClass(CandidateSupportMapper.class);
    job.setReducerClass(AssociationRuleReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Determine input and output paths based on whether the operation is local or S3
    String interPath = outputPath + Constants.FILE_SEPARATOR + Constants.ITERATION_PREFIX + (iteration - 1) + Constants.FILE_SEPARATOR + Constants.USERS_DIR;
    TextInputFormat.addInputPaths(job, interPath);
    FileOutputFormat.setOutputPath(job, new Path(outputPath + Constants.FILE_SEPARATOR + Constants.ITERATION_PREFIX + iteration));

    // Handle cache files
    String joinDirectory = outputPath + Constants.FILE_SEPARATOR + Constants.JOIN_ITERATION_PREFIX + iteration;
    if (IS_LOCAL) {
      File dir = new File(joinDirectory);
      if (dir.exists()) {
        for (String path : dir.list()) {
          if (!path.endsWith(Constants.CRC_EXTENSION) && !path.startsWith(Constants.SUCCESS_FILE)) {
            job.addCacheFile(new URI(joinDirectory + Constants.FILE_SEPARATOR + path));
          }
        }
      }
    } else {
      final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME_STRING).withPrefix(joinDirectory);
      ObjectListing objects = s3Client.listObjects(listObjectsRequest);
      for (S3ObjectSummary summary : objects.getObjectSummaries()) {
        String path = summary.getKey();
        if (!path.endsWith(Constants.CRC_EXTENSION) && !path.startsWith(Constants.SUCCESS_FILE)) {
          logger.info("Added File " + BUCKET_NAME + Constants.FILE_SEPARATOR + path);
          job.addCacheFile(new URI(BUCKET_NAME + Constants.FILE_SEPARATOR + path));
        }
      }
    }

    String movieDirectory = outputPath + Constants.FILE_SEPARATOR + Constants.ITERATION_PREFIX + (iteration - 1) + Constants.FILE_SEPARATOR + Constants.MOVIE_SETS_PATH;

    if (IS_LOCAL) {
      for (String path : new File(movieDirectory).list()) {
        if (!path.endsWith(Constants.CRC_EXTENSION) && !path.startsWith(Constants.SUCCESS_FILE)) {
          job.addCacheFile(new URI(movieDirectory + Constants.FILE_SEPARATOR + path));
        }
      }
    } else {
      final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME_STRING).withPrefix(movieDirectory);
      ObjectListing objects = s3Client.listObjects(listObjectsRequest);
      for (S3ObjectSummary summary : objects.getObjectSummaries()) {
        String path = summary.getKey();
        if (!path.endsWith(Constants.CRC_EXTENSION) && !path.endsWith(Constants.SUCCESS_FILE)) {
          logger.info("ADDED FILE " + BUCKET_NAME + Constants.FILE_SEPARATOR + path);
          job.addCacheFile(new URI(BUCKET_NAME + Constants.FILE_SEPARATOR + path));
        }
      }
    }

    job.getCacheFiles();

    MultipleOutputs.addNamedOutput(job, Constants.OUTPUT_FREQUENCY_MOVIE_SETS, TextOutputFormat.class, LongWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, Constants.NEXT_ITER_MOVIE_SETS, TextOutputFormat.class, LongWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, Constants.NEXT_ITER_USERS, TextOutputFormat.class, LongWritable.class, Text.class);
  }
}
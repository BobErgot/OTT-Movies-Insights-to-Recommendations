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
    return 0;
  }

  private int setupAndRunJob1(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance(getConf(), "User Rating Threshold Filter");
    job.setJarByClass(AprioriFrequentMovieSet.class);
    configureJob1(job, inputPath, outputPath);
    boolean completion = job.waitForCompletion(true);
    return completion ? (int) job.getCounters().findCounter(Constants.Counters.USER_COUNT).getValue() : -1;
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
}
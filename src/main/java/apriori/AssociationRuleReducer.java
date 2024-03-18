package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A reducer class for calculating and writing the support and confidence of movie sets.
 * This class is used in the association rule mining process for identifying strong rules
 * based on the support and confidence metrics computed from the input data.
 */
public class AssociationRuleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private static final Logger logger = LogManager.getLogger(AssociationRuleReducer.class);
  private final DoubleWritable supportValue = new DoubleWritable();
  private final Map<Set<String>, Double> movieSetFrequencyMap = new HashMap<>();
  private double supportThreshold;
  private MultipleOutputs<Text, DoubleWritable> multipleOutputs;
  private int totalUsers;
  private int frequentSetCount = 0;
  private int currentIteration;
  private String outputPrefix;

  /**
   * Setup method to configure the reducer with necessary settings from the job configuration.
   * This method prepares the reducer to process data by loading necessary configuration
   * parameters and preparing access to the file system.
   *
   * @param context The context of the current job.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    supportThreshold = config.getDouble(Constants.SUPPORT_THRESHOLD, Constants.DEFAULT_MAX_VALUE);
    totalUsers = config.getInt(Constants.TOTAL_USERS, Constants.DEFAULT_MAX_VALUE);
    multipleOutputs = new MultipleOutputs<>(context);
    currentIteration = config.getInt(Constants.CURRENT_ITERATION, -1);

    FileSystem fileSystem = initializeFileSystem(config);

    for (URI cacheFile : context.getCacheFiles()) {
      if (!cacheFile.getPath().contains(Constants.JOIN_FILE_PREFIX)) {
        loadPreviousSupports(new Path(cacheFile), fileSystem);
      }
    }
  }

  /**
   * Initializes the file system based on the job's configuration.
   *
   * @param config The configuration of the current job.
   * @return A FileSystem instance configured for the job.
   * @throws IOException If an error occurs while setting up the file system.
   */
  private FileSystem initializeFileSystem(Configuration config) throws IOException {
    String uriPath = config.getBoolean(Constants.IS_LOCAL, true) ? "." : config.get(Constants.BUCKET_NAME);
    outputPrefix = config.getBoolean(Constants.IS_LOCAL, true) ? "" : uriPath + Constants.FILE_SEPARATOR + Constants.ITERATION_PREFIX + currentIteration + Constants.FILE_SEPARATOR;
    try {
      return FileSystem.get(new URI(uriPath), config);
    } catch (URISyntaxException e) {
      throw new IOException("Error setting up file system.", e);
    }
  }

  /**
   * Loads previously calculated support values for movie sets from a specified file path.
   *
   * @param filePath   The path to the file containing the support values.
   * @param fileSystem The file system to use for reading the file.
   * @throws IOException If an error occurs during file reading.
   */
  private void loadPreviousSupports(Path filePath, FileSystem fileSystem) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)))) {
      String row;
      while ((row = reader.readLine()) != null) {
        if (!row.isEmpty()) {
          String[] parts = row.split("\t");
          if (parts.length == 2) {
            Set<String> movieSet = new HashSet<>(Arrays.asList(parts[0].split(",")));
            double support = Double.parseDouble(parts[1]);
            movieSetFrequencyMap.put(movieSet, support);
          } else {
            logger.error("Row does not contain expected number of parts (2): " + row);
          }
        }
      }
    }
  }

  /**
   * Reduces a set of input values into a single output record of support and confidence
   * for movie sets.
   *
   * @param key     The key representing a movie set.
   * @param values  The values representing support counts.
   * @param context The context for writing the output of the reducer.
   */
  @Override
  public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
    int count = 0;
    for (DoubleWritable value : values) {
      count += (int) value.get();
    }

    double support = (double) count / totalUsers;
    if (support > supportThreshold) {
      supportValue.set(support);
      writeFrequentMovieSet(key);

      Set<String> currentMovieSet = new HashSet<>(Arrays.asList(key.toString().split(",")));
      calculateAndWriteConfidence(currentMovieSet, support, context);
    }
  }

  /**
   * Writes a movie set that meets the frequency threshold to the output.
   *
   * @param movieSet The movie set to write.
   * @throws IOException          If an error occurs during writing.
   * @throws InterruptedException If the thread is interrupted during processing.
   */
  private void writeFrequentMovieSet(Text movieSet) throws IOException, InterruptedException {
    multipleOutputs.write(Constants.NEXT_ITER_MOVIE_SETS, movieSet, supportValue, outputPrefix + Constants.MOVIE_SETS_PART_PATH);
  }

  /**
   * Calculates and writes the confidence for each derived association rule from a movie set.
   *
   * @param currentMovieSet The current movie set from which to derive rules.
   * @param support         The support value for the movie set.
   * @param context         The context to write the confidence values.
   * @throws IOException          If an error occurs during writing.
   * @throws InterruptedException If the thread is interrupted during processing.
   */
  private void calculateAndWriteConfidence(Set<String> currentMovieSet, double support, Context context) throws IOException, InterruptedException {
    frequentSetCount++;
    for (String movieId : currentMovieSet) {
      Set<String> subset = new HashSet<>(currentMovieSet);
      subset.remove(movieId);
      if (movieSetFrequencyMap.containsKey(subset)) {
        double confidence = support / movieSetFrequencyMap.get(subset);
        multipleOutputs.write(Constants.OUTPUT_FREQUENCY_MOVIE_SETS, new Text(subset + " -> " + movieId), new DoubleWritable(confidence), outputPrefix + Constants.OUTPUT_CONFIDENCES_PART);
      }
    }
  }

  /**
   * Cleanup method that finalizes counters and closes multiple outputs.
   *
   * @param context The context of the job.
   * @throws IOException          If an error occurs during cleanup.
   * @throws InterruptedException If the thread is interrupted during cleanup.
   */
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter(Constants.Counters.MOVIE_SET_COUNT).increment(frequentSetCount);
    multipleOutputs.close();
  }
}
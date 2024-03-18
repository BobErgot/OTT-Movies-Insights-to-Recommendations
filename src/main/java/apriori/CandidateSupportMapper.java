package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This Mapper class is responsible for processing input data to generate and emit movie sets
 * that have previously been determined as frequent. It checks the current set of movies against
 * cached frequent sets and outputs those that meet the criteria.
 *
 * <p>Outputs are tagged for further iteration or analysis, allowing the MapReduce job to dynamically
 * adapt based on the found frequent sets.
 */
public class CandidateSupportMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private final DoubleWritable one = new DoubleWritable(1);
  private final Set<Set<String>> previousFrequentMovieSets = new HashSet<>();
  private MultipleOutputs<Text, DoubleWritable> multipleOutputs;
  private String outputPrefix;
  private int currentIteration;

  /**
   * Initializes the mapper, setting up necessary configurations and loading previous frequent
   * movie sets into memory for comparison with current data during the map phase.
   *
   * @param context The Hadoop job context
   * @throws IOException          If an error occurs during file system operations
   * @throws InterruptedException If operation is interrupted during execution
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    currentIteration = config.getInt(Constants.CURRENT_ITERATION, -1);
    multipleOutputs = new MultipleOutputs<>(context);

    FileSystem fileSystem = initializeFileSystem(config);

    for (URI cacheFile : context.getCacheFiles()) {
      if (cacheFile.getPath().contains(Constants.JOIN_FILE_PREFIX)) {
        readFrequentSets(new Path(cacheFile), fileSystem);
      }
    }
  }

  /**
   * Initializes the FileSystem based on configuration settings to access HDFS or local files.
   *
   * @param config The Hadoop configuration settings
   * @return FileSystem configured based on the given settings
   * @throws IOException If an error occurs during setup
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
   * Reads sets of movies that were frequently occurring in previous iterations from the HDFS.
   *
   * @param filePath   The path to the file containing the frequent sets
   * @param fileSystem The file system to use for reading the data
   * @throws IOException If an error occurs during read operations
   */
  private void readFrequentSets(Path filePath, FileSystem fileSystem) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)))) {
      String row;
      while ((row = reader.readLine()) != null) {
        if (!row.isEmpty()) {
          Set<String> movieSet = new HashSet<>(Arrays.asList(row.split(",")));
          previousFrequentMovieSets.add(movieSet);
        }
      }
    }
  }

  /**
   * Processes input records, matching current movie sets against previously identified frequent sets.
   * Outputs the frequent sets found in the current data batch for further processing.
   *
   * @param key       The input key, usually representing the position in the file
   * @param movieData The text data of current movie sets
   * @param context   The context to write output to
   * @throws IOException          If an error occurs during writing outputs
   * @throws InterruptedException If the map operation is interrupted
   */
  @Override
  public void map(final LongWritable key, final Text movieData, final Context context) throws IOException, InterruptedException {
    String[] record = movieData.toString().split("\t");
    Set<String> currentSet = new HashSet<>(Arrays.asList(record[1].split(",")));

    boolean isFrequentSetFound = false;
    for (Set<String> previousSet : previousFrequentMovieSets) {
      if (currentSet.containsAll(previousSet)) {
        isFrequentSetFound = true;
        context.write(new Text(String.join(",", previousSet)), one);
      }
    }
    if (isFrequentSetFound) {
      multipleOutputs.write(Constants.NEXT_ITER_USERS, new Text(record[0]), new Text(record[1]), outputPrefix + Constants.USERS_PART_PATH);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (null != multipleOutputs) {
      multipleOutputs.close();
    }
  }
}

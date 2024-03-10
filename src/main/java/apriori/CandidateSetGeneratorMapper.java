package apriori;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mapper class for generating candidate movie sets in Apriori algorithm
 * This class reads the sets of frequent movie sets from previous iterations, stored in HDFS,
 * and generates new candidate movie sets by combining these with current set of movies in each
 * input.
 */
public class CandidateSetGeneratorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  private final List<Set<String>> previousFrequentMovieSets = new ArrayList<>();
  private int currentIteration;
  private FileSystem fileSystem;

  /**
   * Setup method that initializes file system configuration and reads previous frequent movie sets
   * into memory before the actual mapping process.
   *
   * @param context The Hadoop context that provides access to configuration parameters and other utilities.
   * @throws IOException          if there's a failure in reading input data.
   * @throws InterruptedException if the job is interrupted.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    currentIteration = config.getInt(Constants.CURRENT_ITERATION, -1);
    initializeFileSystem(config);

    for (URI cacheFile : context.getCacheFiles()) {
      readFrequentItemSets(new Path(cacheFile));
    }
  }

  /**
   * Initializes the FileSystem object based on the provided configuration.
   *
   * @param config Configuration parameters from the Hadoop job.
   * @throws IOException if there is an error setting up the FileSystem.
   */
  private void initializeFileSystem(Configuration config) throws IOException {
    String uriPath = config.getBoolean(Constants.IS_LOCAL, true) ? "." : config.get(Constants.BUCKET_NAME);
    try {
      fileSystem = FileSystem.get(new URI(uriPath), config);
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Reads and parses previous frequent movie sets from HDFS into memory.
   *
   * @param filePath Path to the file containing previous frequent movie sets.
   * @throws IOException if there is an error reading the file.
   */
  private void readFrequentItemSets(Path filePath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isEmpty()) {
          Set<String> movieSet = new HashSet<>(Arrays.asList(line.split("\t")[0].split(",")));
          previousFrequentMovieSets.add(movieSet);
        }
      }
    }
  }

  /**
   * Map method to generate new candidate movie sets from the combination of previous frequent
   * movie sets and current input.
   * Emits the new movie sets if they meet the size criteria defined by the current iteration.
   *
   * @param key       Input key, typically the byte offset of the input data.
   * @param movieData Text value containing the current transaction i.e. movie set
   * @param context   The Hadoop context to write the output to the next stage.
   * @throws IOException          if there's an error during map execution.
   * @throws InterruptedException if the job is interrupted.
   */
  @Override
  public void map(final LongWritable key, final Text movieData, final Context context) throws IOException, InterruptedException {
    Set<String> currentSet = new HashSet<>(Arrays.asList(movieData.toString().split("\t")[0].split(",")));

    for (Set<String> previousSet : previousFrequentMovieSets) {
      Set<String> union = Sets.union(currentSet, previousSet);
      if (union.size() == currentIteration) {
        String[] unionArray = union.toArray(new String[0]);
        Arrays.sort(unionArray);
        context.write(new Text(String.join(",", unionArray)), NullWritable.get());
      }
    }
  }
}

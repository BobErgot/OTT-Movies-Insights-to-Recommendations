package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer class that aggregates movie IDs for each user into a comma-separated list.
 * This class writes the output to a specified directory, organizing data based on the iteration of the processing.
 */
public class UserMovieAggregationReducer extends Reducer<Text, Text, Text, Text> {
  private MultipleOutputs<Text, Text> multipleOutputs;
  private final StringBuilder outputPrefixBuilder = new StringBuilder();

  /**
   * Setup method to initialize the reducer with necessary parameters from the job configuration.
   * Prepares the output prefix based on the iteration and location settings.
   *
   * @param context The Hadoop context for accessing configuration and other job-specific parameters.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();

    multipleOutputs = new MultipleOutputs<>(context);

    int currentIteration = config.getInt("currentIteration", -1);

    if (!config.getBoolean(Constants.IS_LOCAL, true)) {
      outputPrefixBuilder.append(config.get(Constants.BUCKET_NAME)).append(Constants.FILE_SEPARATOR).append(Constants.ITERATION_PREFIX).append(currentIteration).append(Constants.FILE_SEPARATOR);
    }
  }

  /**
   * Reduce method to process input key/value pairs.
   * Aggregates all movie IDs associated with a user and writes them to an output file.
   *
   * @param userId  The key from the input data, representing a user ID.
   * @param values  The values associated with the key, representing movie IDs.
   * @param context The Hadoop context to write the output to the next stage.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void reduce(final Text userId, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
    List<String> movieIdList = new ArrayList<>();
    for (Text movieId : values) {
      movieIdList.add(movieId.toString());
    }
    multipleOutputs.write(Constants.USERS_DIR, userId, new Text(String.join(",", movieIdList)), outputPrefixBuilder.toString() + Constants.USERS_PART_PATH);
  }

  /**
   * Cleanup method called after the reduce task has processed the last record.
   * It is used to close the MultipleOutputs object.
   *
   * @param context The Hadoop context used for accessing features like counters.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
  }
}

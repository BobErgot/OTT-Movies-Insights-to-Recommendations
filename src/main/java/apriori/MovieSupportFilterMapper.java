package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Mapper class that calculates the frequency of movie lists (movie IDs) based on user
 * interactions, filtering them based on a support threshold that relates to the total number of
 * users.
 */
public class MovieSupportFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
  private final Text movieIdText = new Text();
  private final StringBuilder outputPrefixBuilder = new StringBuilder();
  private int maxUserId;
  private int maxMovieId;
  private double supportThreshold;
  private int totalUsers;
  private int movieSetCount = 0;
  private MultipleOutputs<Text, Text> multipleOutputs;

  /**
   * Setup method to configure the mapper with necessary parameters from the job configuration.
   * Initializes parameters such as maximum user ID, movie ID, support threshold, and sets up multiple outputs.
   *
   * @param context The Hadoop context for accessing configuration and other job-specific parameters.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    maxUserId = config.getInt(Constants.MAX_USER_ID, Constants.DEFAULT_MAX_VALUE);
    maxMovieId = config.getInt(Constants.MAX_MOVIE_ID, Constants.DEFAULT_MAX_VALUE);
    supportThreshold = config.getDouble(Constants.SUPPORT_THRESHOLD, Constants.DEFAULT_MAX_VALUE);
    totalUsers = config.getInt(Constants.TOTAL_USERS, Constants.DEFAULT_MAX_VALUE);
    multipleOutputs = new MultipleOutputs<>(context);

    int currentIteration = config.getInt(Constants.CURRENT_ITERATION, -1);

    if (!config.getBoolean(Constants.IS_LOCAL, true)) {
      outputPrefixBuilder.append(config.get(Constants.BUCKET_NAME)).append(Constants.FILE_SEPARATOR).append(Constants.ITERATION_PREFIX).append(currentIteration).append(Constants.FILE_SEPARATOR);
    }
  }

  /**
   * The map method where the actual processing of input data happens. It reads movie ID and user interactions,
   * filters based on the configured maximum IDs, and calculates the support for each movie.
   * Outputs the result if the calculated support is above the threshold.
   *
   * @param key     The input key, typically representing the byte offset of the line in the input file.
   * @param value   The line from the input file which contains movie ID followed by user IDs.
   * @param context The Hadoop context to write the output to the next stage.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
    String[] rows = value.toString().split(":");
    int currentMovieId = Integer.parseInt(rows[0]);

    if (currentMovieId > maxMovieId) return;

    movieIdText.set(rows[0]);
    String[] ratingRecords = rows[1].trim().split("\n");
    List<String> filteredUserIdList = new ArrayList<>();
    for (String rating : ratingRecords) {
      String[] splitRecord = rating.split(",");
      int userId = Integer.parseInt(splitRecord[0]);
      if (userId < maxUserId) {
        filteredUserIdList.add(splitRecord[0]);
      }
    }
    double support = (double) filteredUserIdList.size() / totalUsers;
    if (support > supportThreshold) {
      for (String userId : filteredUserIdList) {
        context.write(new Text(userId), movieIdText);
      }
      multipleOutputs.write(Constants.MOVIES_DIR, movieIdText, new DoubleWritable(support), outputPrefixBuilder + Constants.MOVIE_SETS_PART_PATH);
      movieSetCount++;
    }
  }

  /**
   * Cleanup method is called after the map method has processed the last record.
   * It is used here to increment the counter and close multiple outputs.
   *
   * @param context The Hadoop context used for accessing counters and other job-specific features.
   * @throws IOException          If an I/O error occurs.
   * @throws InterruptedException If the method is interrupted by another thread.
   */
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter(Constants.Counters.MOVIE_SET_COUNT).increment(movieSetCount);
    multipleOutputs.close();
  }
}
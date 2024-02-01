package apriori;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This reducer class is used to count the number of unique users in the dataset.
 * It operates over key-value pairs where keys represent user IDs and values are ignored (null).
 * The unique count of user IDs is calculated and stored in a Hadoop counter.
 */
public class UserCounterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
  private Counter userCount;
  private int totalUsers;

  /**
   * Setup method initializes the counter that will store the count of unique users.
   * This method is called once before the reduce process begins.
   *
   * @param context Provides access to the Hadoop job configuration and the counters.
   * @throws IOException          If an input or output error occurs
   * @throws InterruptedException If the operation is interrupted
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    userCount = context.getCounter(Constants.Counters.USER_COUNT);
    totalUsers = 0;
  }

  /**
   * The reduce method is called once for each unique key (user ID).
   * Every call increases the total user count by one.
   *
   * @param key     The user ID
   * @param values  Not used in this context, since only the key (user ID) matters.
   * @param context Provides the means to write the output and report progress.
   * @throws IOException          If an input or output error occurs
   * @throws InterruptedException If the operation is interrupted
   */
  @Override
  public void reduce(final Text key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
    totalUsers++;
  }

  /**
   * Cleanup method is called after all the key-value pairs have been processed.
   * It is used to finalize the count of total users and update the counter in the job context.
   *
   * @param context Provides access to the Hadoop job configuration and the counters.
   * @throws IOException          If an input or output error occurs
   * @throws InterruptedException If the operation is interrupted
   */
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    userCount.increment(totalUsers);
  }
}
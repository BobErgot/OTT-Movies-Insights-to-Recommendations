package apriori;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This Reducer class outputs each unique frequent movie set received from the Map phase.
 * It directly writes frequent movie sets to the output, ensuring each movie set is recorded once.
 * This class is crucial in the final stage of the Apriori algorithm to finalize the list of frequent movie sets.
 */
public class MovieSetFinalizerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  /**
   * Outputs each unique frequent movie set. The reduce method ignores the NullWritable values
   * and directly emits each key to finalize the list of movie sets.
   *
   * @param key     The frequent movie set as Text.
   * @param values  Ignored in this implementation.
   * @param context Provides the means to write the output.
   * @throws IOException          If writing the output fails.
   * @throws InterruptedException If the job is interrupted.
   */
  @Override
  public void reduce(final Text key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}

package apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This mapper processes movie rating data to filter and emit user IDs based on configured limits.
 * It expects data formatted with a movie ID followed by user ratings. Each rating line includes
 * a user ID, rating, and date. Only user IDs below a specified maximum are emitted.
 * The data format expected is:
 * <pre>
 * movie_id:
 * user_id, rating, date
 * user_id2, rating, date
 * ...
 * </pre>
 */
public class DataPreprocessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  private Integer maxUserId;
  private Integer maxMovieId;
  private static final Logger logger = LogManager.getLogger(DataPreprocessMapper.class);

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    maxUserId = config.getInt(Constants.MAX_USER_ID, Constants.DEFAULT_MAX_VALUE);
    maxMovieId = config.getInt(Constants.MAX_MOVIE_ID, Constants.DEFAULT_MAX_VALUE);
  }

  @Override
  public void map(LongWritable index, Text value, Context context) throws IOException, InterruptedException {
    String[] rows = value.toString().split(":");
    try {
      int currentMovieId = Integer.parseInt(rows[0].trim());
      if (currentMovieId <= maxMovieId) {
        String[] records = rows[1].trim().split("\\n");
        for (String record : records) {
          String[] splitRecord = record.split(",");
          String userId = splitRecord[0].trim();
          if (Integer.parseInt(userId) <= maxUserId) {
            context.write(new Text(userId), NullWritable.get());
          }
        }
      }
    } catch (NumberFormatException e) {
      logger.error("Skipping invalid input: " + value, e);
    }
  }
}
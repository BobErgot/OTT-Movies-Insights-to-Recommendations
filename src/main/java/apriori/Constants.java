package apriori;

import java.io.File;

public class Constants {
  public static final String MAX_USER_ID = "maxUserId";
  public static final String MAX_MOVIE_ID = "maxMovieId";
  public static final int DEFAULT_MAX_VALUE = Integer.MAX_VALUE;
  public static final String SUPPORT_THRESHOLD = "supportThreshold";
  public static final String TOTAL_USERS = "totalUsers";
  public static final String CURRENT_ITERATION = "currentIteration";
  public static final String IS_LOCAL = "isLocal";
  public static final String BUCKET_NAME = "bucketName";
  public static final String MOVIES_DIR = "movies";
  public static final String USERS_DIR = "users";
  public static final String ITERATION_PREFIX = "iteration_";
  public static final String JOIN_ITERATION_PREFIX = "join_iteration_";
  public static final String CRC_EXTENSION = ".crc";
  public static final String SUCCESS_FILE = "_SUCCESS";
  public static final String FILE_SEPARATOR = File.separator;
  public static final String MOVIE_SETS_PATH = "movie_sets";
  public static final String MOVIE_SETS_PART_PATH = MOVIE_SETS_PATH + File.separator + "part";
  public static final String USERS_PART_PATH = "users" + File.separator + "part";
  public static final String OUTPUT_CONFIDENCES = "confidences";

  public enum Counters {
    USER_COUNT, MOVIE_SET_COUNT
  }
}
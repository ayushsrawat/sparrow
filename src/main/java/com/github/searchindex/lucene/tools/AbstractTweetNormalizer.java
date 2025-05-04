package com.github.searchindex.lucene.tools;

import com.github.searchindex.lucene.TweetNormalizer;
import com.github.searchindex.lucene.entry.Tweet;
import com.github.searchindex.util.ParseUtil;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@PropertySource("classpath:index.properties")
public abstract class AbstractTweetNormalizer implements TweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(AbstractTweetNormalizer.class);
  private static final Integer TWEET_BATCH_SIZE = 5000;
  private final ParseUtil parseUtil;

  public AbstractTweetNormalizer(ParseUtil parseUtil) {
    this.parseUtil = parseUtil;
  }

  @Value("${dataset.path}")
  protected String datasetDirectory;
  @Value("${twitter.dataset.v1.csv.path}")
  protected String twitterDatasetV1;
  @Value("${twitter.dataset.v2.csv.path}")
  protected String twitterDatasetV2;
  @Value("${twitter.dataset.v3.csv.path}")
  protected String twitterDatasetV3;

  /**
   * this approach had two problems -
   * <ul>
   *   <li> running this is very costly, when reading 100,000 tweets long file, it keeps running for an hr </li>
   *   <li> since the tweets list gets very long, last time I noticed it leaked some of the tweets </li>
   * </ul>
   *
   * solutions -
   * <ul>
   *   <li> instead of saving parse tweets in memory, accept a handler and batch execute those tweets to handler</li>
   *   <li> reading a large file from resource stream is very costly operation, read it directly from file system</li>
   *   <li> use uniVo_city-parsers, they parse in stream and faster than apache-csv-parser </li>
   * </ul>
   */
  protected Integer normalizeTwitterDataset(BiConsumer<List<Tweet>, String> batchHandler, String datasetFilePath) {
    Path dataPath = Paths.get(datasetDirectory + datasetFilePath);
    if (!Files.exists(dataPath)) {
      logger.error("{} dataset not found.", dataPath);
      return 0;
    }
    logger.info("[{}] Starting normalization...", datasetFilePath);
    try (BufferedReader reader = Files.newBufferedReader(dataPath)) {
      CsvParserSettings settings = new CsvParserSettings();
      settings.setHeaderExtractionEnabled(true);
      settings.setSkipEmptyLines(true);
      settings.setMaxCharsPerColumn(10_000);
      settings.setProcessorErrorHandler((error, inputRow, context) ->
        logger.warn("Row parsing error at row {}: {}", context.currentRecord(), error.getMessage()));

      List<Tweet> tweetsBatch = new ArrayList<>(TWEET_BATCH_SIZE);
      AtomicInteger tweetCount = new AtomicInteger(0);
      settings.setProcessor(new AbstractRowProcessor() {
        private int batchCount = 0;
        @Override
        public void rowProcessed(String[] row, ParsingContext context) {
          Map<String, String> rowMap = context.headers() != null
            ? toRowMap(context.headers(), row)
            : Collections.emptyMap();
          Tweet tweet = getTweetFromRow(datasetFilePath, rowMap);
          tweetsBatch.add(tweet);
          tweetCount.incrementAndGet();
          if (tweetsBatch.size() >= TWEET_BATCH_SIZE) {
            batchCount++;
            batchHandler.accept(new ArrayList<>(tweetsBatch),
              generateOutputJsonFileName(datasetFilePath, batchCount));
            logger.info("[{}] Saving tweet batch: {} of size: {}", datasetFilePath, batchCount, tweetsBatch.size());
            tweetsBatch.clear();
          }
        }

        @Override
        public void processEnded(ParsingContext context) {
          if (!tweetsBatch.isEmpty()) {
            batchHandler.accept(new ArrayList<>(tweetsBatch), generateOutputJsonFileName(datasetFilePath, batchCount));
            logger.info("Saving final tweet batch: {} of size: {}", batchCount, tweetsBatch.size());
            tweetsBatch.clear();
          }
          logger.info("Finished parsing tweets.");
        }
      });
      CsvParser parser = new CsvParser(settings);
      parser.parse(reader);
      return tweetCount.get();
    } catch (IOException ioe) {
      logger.error("Error normalizing the v1 dataset : {}", ioe.getMessage());
      return 0;
    }
  }

  private Map<String, String> toRowMap(String[] headers, String[] row) {
    Map<String, String> map = new HashMap<>(headers.length);
    for (int i = 0; i < headers.length; i++) {
      map.put(headers[i], row[i]);
    }
    return map;
  }

  private Tweet getTweetFromRow(String datasetFilePath, Map<String, String> row) {
    if (datasetFilePath.equals(twitterDatasetV1)) {
      return Tweet.builder()
        .tweetId(parseUtil.parseLong(row.get("id")))
        .username(rowGet(row,"username"))
        .tweet(rowGet(row,"text"))
        .tweetDate(parseUtil.parseDateV1(row.get("date")))
        .build();
    }
    if (datasetFilePath.equals(twitterDatasetV2)) {
      return Tweet.builder()
        .tweetId(parseUtil.parseLong(row.get("id")))
        .username(rowGet(row, "user_posted"))
        .fullName(rowGet(row,"name"))
        .tweet(rowGet(row,"description"))
        .tweetDate(parseUtil.parseDateV2(row.get("date_posted")))
        .url(row.get("url"))
        .views(parseUtil.parseInt(row.get("views")))
        .likes(parseUtil.parseInt(row.get("likes")))
        .retweets(parseUtil.parseInt(row.get("reposts")))
        .build();
    }
    if (datasetFilePath.equals(twitterDatasetV3)) {
      return Tweet.builder()
        .tweetId(parseUtil.parseLong(row.get("Tweet_ID")))
        .username(rowGet(row,"Username"))
        .tweet(rowGet(row,"Text"))
        .likes(parseUtil.parseInt(row.get("Likes")))
        .retweets(parseUtil.parseInt(row.get("Retweets")))
        .tweetDate(parseUtil.parseDateV3(row.get("Timestamp")))
        .build();
    }
    throw new RuntimeException("Error getting tweet row, dataset not supported.");
  }

  private String generateOutputJsonFileName(String datasetFilePath, int batchCount) {
    if (this instanceof TweetDBNormalizer) return null;
    // /tweets/twitter-dataset-v1.csv -> /twitter-dataset-v1-batch-1.json
    String saveFileName = datasetFilePath.substring(datasetFilePath.lastIndexOf("/"));
    saveFileName = saveFileName.replace(".csv", "");
    saveFileName += "-batch-" + batchCount + ".json";
    return saveFileName;
  }

  private String rowGet(Map<String, String> row, String key) {
    if (row.get(key) == null) return "";
    return row.get(key);
  }

}

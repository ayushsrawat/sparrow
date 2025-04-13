package com.github.searchindex.lucene.core.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.searchindex.lucene.core.entry.Tweet;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * normalize the tweets data, in order to make it parsable <br>
 * TweetNormalizer reads the csv datasets and convert them to parsable json dataset
 */
@Service
@RequiredArgsConstructor
@PropertySource("classpath:index.properties")
public class TweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(TweetNormalizer.class);

  @Value("${tweet.output.json.path}")
  private String twitterDatasetDirectory;

  @Value("${twitter.dataset.v1.csv.path}")
  private String twitterDatasetV1;

  @Value("${twitter.dataset.v2.csv.path}")
  private String twitterDatasetV2;

  @Value("${twitter.dataset.v3.csv.path}")
  private String twitterDatasetV3;

  private final ObjectMapper objectMapper;
  private final ExecutorService executorService;
  private final ParseUtil parseUtil;

  public void normalizeCsv() {
    CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> {
      List<Tweet> tweets = normalizeTwitterV1Dataset();
      if (!tweets.isEmpty()) {
        saveTweetsToJson(tweets, "twitter-dataset-v1.json");
      }
    }, executorService);
    CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> {
      List<Tweet> tweets = normalizeTwitterV2Dataset();
      if (!tweets.isEmpty()) {
        saveTweetsToJson(tweets, "twitter-dataset-v2.json");
      }
    }, executorService);
    CompletableFuture<Void> f3 = CompletableFuture.runAsync(() -> {
      List<Tweet> tweets = normalizeTwitterV3Dataset();
      if (!tweets.isEmpty()) {
        saveTweetsToJson(tweets, "twitter-dataset-v3.json");
      }
    }, executorService);
    CompletableFuture.allOf(f1, f2, f3).join();
    //todo : can't see the logs of f2, and f3 because of f1 thread
    logger.info("Normalized tweets successfully!");
  }

  private void saveTweetsToJson(List<Tweet> tweets, String filename) {
    Path resource = Paths.get(twitterDatasetDirectory);
    if (!resource.toFile().exists()) {
      logger.error("{} dir not found! Please change project resource path {} in {} ", twitterDatasetDirectory, "tweet.output.json.path", "index.properties");
      return;
    }

    File tweetsJsonFile = resource.resolve("tweets").resolve(filename).toFile();
    try {
      File parent = tweetsJsonFile.getParentFile();
      if (!parent.exists() && !parent.mkdirs()) {
        logger.error("Error creating directory for file {}", parent);
        return;
      }
      objectMapper
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .writerWithDefaultPrettyPrinter()
        .writeValue(tweetsJsonFile, tweets);
      logger.info("Saved {} tweets to {}", tweets.size(), tweetsJsonFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Error saving {} tweets to {} file : {} ", tweets.size(), filename, e.getMessage());
    }
  }

  private List<Tweet> normalizeTwitterV1Dataset() {
    try (InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV1)) {
      if (resourceAsStream == null) {
        logger.error("v1 dataset not found.");
        return List.of();
      }
      if (this.getClass().getResource(twitterDatasetV1.replace(".csv", ".json")) != null) {
        logger.warn("Normalized file found in resources! Skipping normalizing v1 dataset");
        return List.of();
      }
      List<Tweet> tweets = new ArrayList<>();
      try (
        Reader reader = new InputStreamReader(resourceAsStream);
        CSVParser csvParser = CSVParser.builder()
          .setReader(reader)
          .setFormat(CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get())
          .get()) {
        for (CSVRecord record : csvParser) {
          Tweet tweet = Tweet.builder()
            .tweetId(parseUtil.parseLong(recordGet(record, "id")))
            .username(recordGet(record, "username"))
            .tweet(recordGet(record, "text"))
            .tweetDate(parseUtil.parseDateV1(recordGet(record, "date")))
            .build();
          tweets.add(tweet);
          logger.info("Parsing tweet : {} from v1 dataset", tweet);
        }
      }
      return tweets;
    } catch (IOException ioe) {
      logger.error("Error normalizing the v1 dataset : {}", ioe.getMessage());
      return List.of();
    }
  }

  private List<Tweet> normalizeTwitterV2Dataset() {
    try (InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV2)) {
      if (resourceAsStream == null) {
        logger.error("v2 dataset not found.");
        return List.of();
      }
      if (this.getClass().getResource(twitterDatasetV2.replace(".csv", ".json")) != null) {
        logger.warn("Normalized file found in resources! Skipping normalizing v2 dataset");
        return List.of();
      }
      List<Tweet> tweets = new ArrayList<>();
      try (
        Reader reader = new InputStreamReader(resourceAsStream);
        CSVParser csvParser = CSVParser.builder()
          .setReader(reader)
          .setFormat(CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get())
          .get()) {
        for (CSVRecord record : csvParser) {
          Tweet tweet = Tweet.builder()
            .tweetId(parseUtil.parseLong(recordGet(record, "id")))
            .username(recordGet(record, "user_posted"))
            .fullName(recordGet(record, "name"))
            .tweet(recordGet(record, "description"))
            .tweetDate(parseUtil.parseDateV2(recordGet(record, "date_posted")))
            .url(recordGet(record, "url"))
            .views(parseUtil.parseInt(recordGet(record, "views")))
            .likes(parseUtil.parseInt(recordGet(record, "likes")))
            .retweets(parseUtil.parseInt(recordGet(record, "reposts")))
            .build();
          tweets.add(tweet);
          logger.info("Parsing tweet : {} from v2 dataset", tweet);
        }
      }
      return tweets;
    } catch (IOException ioe) {
      logger.error("Error normalizing the v2 dataset : {}", ioe.getMessage());
      return List.of();
    }
  }

  private List<Tweet> normalizeTwitterV3Dataset() {
    InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV3);
    if (resourceAsStream == null) {
      logger.error("v3 dataset not found.");
      return List.of();
    }
    if (this.getClass().getResource(twitterDatasetV3.replace(".csv", ".json")) != null) {
      logger.warn("Normalized file found in resources! Skipping normalizing v3 dataset");
      return List.of();
    }
    List<Tweet> tweets = new ArrayList<>();
    try (
      Reader reader = new InputStreamReader(resourceAsStream);
      CSVParser csvParser = CSVParser.builder()
        .setReader(reader)
        .setFormat(CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get())
        .get()) {
      for (CSVRecord record : csvParser) {
        Tweet tweet = Tweet.builder()
          .tweetId(parseUtil.parseLong(recordGet(record, "Tweet_ID")))
          .username(recordGet(record, "Username"))
          .tweet(recordGet(record, "Text"))
          .likes(parseUtil.parseInt(recordGet(record, "Likes")))
          .retweets(parseUtil.parseInt(recordGet(record, "Retweets")))
          .tweetDate(parseUtil.parseDateV3(recordGet(record, "Timestamp")))
          .build();
        tweets.add(tweet);
        logger.info("Parsing tweet : {} from v3 dataset", tweet);
      }
      resourceAsStream.close();
    } catch (IOException ioe) {
      logger.error("Error normalizing the v3 dataset : {}", ioe.getMessage());
      return List.of();
    }
    return tweets;
  }

  private String recordGet(CSVRecord record, String key) {
    return record.isMapped(key) && record.isSet(key) ? record.get(key) : null;
  }

}

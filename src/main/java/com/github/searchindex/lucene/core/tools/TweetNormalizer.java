package com.github.searchindex.lucene.core.tools;

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
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
  private static final String TWITTER_DATASET_V1 = "/tweets/twitter-dataset-v1.csv";
  private static final String TWITTER_DATASET_V2 = "/tweets/twitter-dataset-v2.csv";
  private static final String TWITTER_DATASET_V3 = "/tweets/twitter-dataset-v3.csv";

  @Value("${tweet.output.json.path}")
  private String twitterDatasetDirectory;

  private final ObjectMapper objectMapper;
  private final ExecutorService executorService;

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
    logger.info("Normalized tweets successfully! Generated");
  }

  private void saveTweetsToJson(List<Tweet> tweets, String filename) {
    Path resource = Paths.get(twitterDatasetDirectory);
    if (!resource.toFile().exists()) {
      logger.error("{} dir not found! Please change project resource path {} in {} ", twitterDatasetDirectory, "tweet.output.json.path", "index.properties");
      return;
    }

    File tweetsJsonFile = resource.resolve("tweet").resolve(filename).toFile();
    try {
      File parent = tweetsJsonFile.getParentFile();
      if (!parent.exists() && !parent.mkdirs()) {
        logger.error("Error creating directory for file {}", parent);
        return;
      }
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(tweetsJsonFile, tweets);
      logger.info("Saved {} tweets to {}", tweets.size(), tweetsJsonFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Error saving {} tweets to {} file : {} ", tweets.size(), filename, e.getMessage());
    }
  }

  // if you found twitter-dataset-v1.json in the resource don't bother to write again
  private List<Tweet> normalizeTwitterV1Dataset() {
    try (InputStream resourceAsStream = getClass().getResourceAsStream(TWITTER_DATASET_V1)) {
      if (resourceAsStream == null) {
        logger.error("v1 dataset not found.");
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
            .tweetId(parseLong(recordGet(record, "id")))
            .username(recordGet(record, "username"))
            .tweet(recordGet(record, "text"))
            .tweetDate(parseDateV1(recordGet(record, "date")))
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
    try (InputStream resourceAsStream = getClass().getResourceAsStream(TWITTER_DATASET_V2)) {
      if (resourceAsStream == null) {
        logger.error("v2 dataset not found.");
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
            .tweetId(parseLong(recordGet(record, "id")))
            .username(recordGet(record, "user_posted"))
            .fullName(recordGet(record, "name"))
            .tweet(recordGet(record, "description"))
            .tweetDate(parseDateV2(recordGet(record, "date_posted")))
            .url(recordGet(record, "url"))
            .views(parseInt(recordGet(record, "views")))
            .likes(parseInt(recordGet(record, "likes")))
            .retweets(parseInt(recordGet(record, "reposts")))
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
    InputStream resourceAsStream = getClass().getResourceAsStream(TWITTER_DATASET_V3);
    if (resourceAsStream == null) {
      logger.error("v3 dataset not found.");
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
          .tweetId(parseLong(recordGet(record, "Tweet_ID")))
          .username(recordGet(record, "Username"))
          .tweet(recordGet(record, "Text"))
          .likes(parseInt(recordGet(record, "Likes")))
          .retweets(parseInt(recordGet(record, "Retweets")))
          .tweetDate(parseDateV3(recordGet(record, "Timestamp")))
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

  private Long parseLong(String value) {
    try {
      return value == null || value.isBlank() ? null : Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  private Integer parseInt(String value) {
    try {
      return value == null || value.isBlank() ? null : Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  //Mon Apr 06 22:19:49 PDT 2009
  private LocalDateTime parseDateV1(String date) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
      ZonedDateTime zdt = ZonedDateTime.parse(date, formatter);
      return zdt.toLocalDateTime();
    } catch (DateTimeException e) {
      logger.warn("Failed to parse V1 date: {}", date);
      return null;
    }
  }

  //2024-05-29T06:30:33.000Z
  private LocalDateTime parseDateV2(String date) {
    try {
      String cleanDate = date.trim().replace("\"", "");
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
      OffsetDateTime offsetDateTime = OffsetDateTime.parse(cleanDate, formatter);
      return offsetDateTime.toLocalDateTime();
    } catch (DateTimeException | NullPointerException e) {
      logger.warn("Failed to parse V2 date: {}", date);
      return null;
    }
  }

  //2023-01-30 11:00:51
  private LocalDateTime parseDateV3(String date) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      return LocalDateTime.parse(date, formatter);
    } catch (DateTimeException e) {
      logger.warn("Failed to parse V3 date: {}", date);
      return null;
    }
  }

}

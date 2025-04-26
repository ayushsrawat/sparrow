package com.github.searchindex.lucene.tools;

import com.github.searchindex.lucene.TweetNormalizer;
import com.github.searchindex.lucene.entry.Tweet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

@PropertySource("classpath:index.properties")
public abstract class AbstractTweetNormalizer implements TweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(AbstractTweetNormalizer.class);
  private final ParseUtil parseUtil;

  public AbstractTweetNormalizer(ParseUtil parseUtil) {
    this.parseUtil = parseUtil;
  }

  @Value("${twitter.dataset.v1.csv.path}")
  private String twitterDatasetV1;

  @Value("${twitter.dataset.v2.csv.path}")
  private String twitterDatasetV2;

  @Value("${twitter.dataset.v3.csv.path}")
  private String twitterDatasetV3;

  protected List<Tweet> normalizeTwitterV1Dataset() {
    try (InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV1)) {
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

  protected List<Tweet> normalizeTwitterV2Dataset() {
    try (InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV2)) {
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

  protected List<Tweet> normalizeTwitterV3Dataset() {
    InputStream resourceAsStream = getClass().getResourceAsStream(twitterDatasetV3);
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

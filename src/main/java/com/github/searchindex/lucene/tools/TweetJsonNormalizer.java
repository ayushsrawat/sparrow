package com.github.searchindex.lucene.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.searchindex.lucene.entry.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
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
@Service("tweetJsonNormalizer")
@PropertySource("classpath:index.properties")
public class TweetJsonNormalizer extends AbstractTweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(TweetJsonNormalizer.class);
  private static final String DATASET_V1_FILENAME = "twitter-dataset-v1.json";
  private static final String DATASET_V2_FILENAME = "twitter-dataset-v2.json";
  private static final String DATASET_V3_FILENAME = "twitter-dataset-v3.json";

  @Value("${tweet.output.json.path}")
  private String twitterDatasetDirectory;


  private final ObjectMapper objectMapper;
  private final ExecutorService executorService;

  public TweetJsonNormalizer(ObjectMapper objectMapper, ExecutorService executorService, ParseUtil parseUtil) {
    super(parseUtil);
    this.objectMapper = objectMapper;
    this.executorService = executorService;
  }

  @Override
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

  @Override
  public List<Tweet> getNormalizedTweets() {
    List<Tweet> tweets = new ArrayList<>();
    tweets.addAll(getTwitterDatasetFromJson(DATASET_V1_FILENAME));
    tweets.addAll(getTwitterDatasetFromJson(DATASET_V2_FILENAME));
    tweets.addAll(getTwitterDatasetFromJson(DATASET_V3_FILENAME));
    return tweets;
  }

  /**
   * writes tweets into given .json file
   */
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

  /**
   * reads tweets from .json file
   */
  private List<Tweet> getTwitterDatasetFromJson(String datasetFileName) {
    File twitterDataset = new File(twitterDatasetDirectory + "/tweets/" + datasetFileName);
    if (!twitterDataset.exists()) {
      logger.error("{} not found.", twitterDataset.getAbsolutePath());
      return List.of();
    }
    try {
      List<Tweet> tweets = objectMapper.readValue(twitterDataset, new TypeReference<>() {});
      logger.info("Found {} tweets from {}", tweets.size(), datasetFileName);
      return tweets;
    } catch (IOException ioe) {
      logger.error("Failed to read tweets from {}: {}", twitterDataset.getName(), ioe.getMessage());
      return List.of();
    }
  }

}

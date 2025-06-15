package com.github.sparrow.lucene.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sparrow.exception.IndexingException;
import com.github.sparrow.lucene.entity.Tweet;
import com.github.sparrow.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * normalize the tweets data, in order to make it parsable <br>
 * TweetNormalizer reads the csv datasets and convert them to parsable json dataset
 */
@Service("tweetJsonNormalizer")
@PropertySource("classpath:sparrow.properties")
public class TweetJsonNormalizer extends AbstractTweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(TweetJsonNormalizer.class);

  @Value("${indexing.parallel}")
  private boolean parallelExecution;

  private final ObjectMapper objectMapper;
  private final ExecutorService executorService;

  public TweetJsonNormalizer(ObjectMapper objectMapper, ExecutorService executorService, ParseUtil parseUtil) {
    super(parseUtil);
    this.objectMapper = objectMapper;
    this.executorService = executorService;
  }

  @Override
  public void normalizeCsv() throws IndexingException {
    try {
      deleteExistingJsonDataset();
      if (parallelExecution) {
        CompletableFuture<Integer> f1 = CompletableFuture.supplyAsync(() ->
          normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV1), executorService);
        CompletableFuture<Integer> f2 = CompletableFuture.supplyAsync(() ->
          normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV2), executorService);
        CompletableFuture<Integer> f3 = CompletableFuture.supplyAsync(() ->
          normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV3), executorService);
        CompletableFuture.allOf(f1, f2, f3).join();
        try {
          int total = f1.get() + f2.get() + f3.get();
          logger.info("Successfully saved {} tweets into {} in parallel execution", total, datasetDirectory);
        } catch (Exception e) {
          logger.error("Error waiting for normalization completion", e);
        }
      } else {
        int total = 0;
        total += normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV1);
        total += normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV2);
        total += normalizeTwitterDataset(this::saveTweetsToJson, twitterDatasetV3);
        logger.info("Successfully saved {} tweets into {} in linear execution", total, datasetDirectory);
      }
      logger.info("Normalized tweets successfully!");
    } catch (Exception e) {
      final String msg = "Error Normalizing Json";
      logger.error(msg, e);
      throw new IndexingException(msg, e);
    }
  }

  /**
   * writes tweets into given .json file
   */
  private void saveTweetsToJson(List<Tweet> tweets, String filename) {
    Path outputPath = Paths.get(datasetDirectory + "/tweets/json" + filename);
    try {
      if (Files.exists(outputPath) && !Files.deleteIfExists(outputPath)) {
        logger.warn("Failed to delete existing file: {}", outputPath.toAbsolutePath());
      }
      Files.createDirectories(outputPath.getParent());
      objectMapper
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .writerWithDefaultPrettyPrinter()
        .writeValue(outputPath.toFile(), tweets);
      logger.info("Saved {} tweets to {}", tweets.size(), outputPath.toAbsolutePath());
    } catch (IOException e) {
      logger.error("Error saving {} tweets to {} file : {} ", tweets.size(), outputPath, e.getMessage());
      throw new RuntimeException("Failed to save tweets to JSON", e);
    }
  }

  private void deleteExistingJsonDataset() throws IOException {
    Path outputPath = Paths.get(datasetDirectory, "tweets", "json");
    if (Files.exists(outputPath) && Files.isDirectory(outputPath)) {
      try (Stream<Path> path = Files.walk(outputPath)) {
        for (Path p : path.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(p);
        }
        logger.info("Successfully deleted json dataset directory: {}", outputPath);
      }
    }
  }

  @Override
  public List<Tweet> getNormalizedTweets() {
    Path twitterDatasetPath = Paths.get(datasetDirectory + "/tweets/json");
    if (!twitterDatasetPath.toFile().exists()) {
      logger.error("{} not found.", twitterDatasetPath.toAbsolutePath());
      return List.of();
    }
    try {
      List<Tweet> tweets = new ArrayList<>();
      Files.walkFileTree(twitterDatasetPath, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
          File current = path.toFile();
          if (current.isFile() && current.getAbsolutePath().endsWith(".json")) {
            List<Tweet> currentTweets = objectMapper.readValue(current, new TypeReference<>() {
            });
            tweets.addAll(currentTweets);
            logger.info("Found {} tweets from {}", currentTweets.size(), current.getAbsolutePath());
          }
          return super.visitFile(path, attrs);
        }
      });
      return tweets;
    } catch (IOException ioe) {
      logger.error("Failed to read tweets from {}: {}", twitterDatasetPath, ioe.getMessage());
      return List.of();
    }
  }

  @Override
  public boolean needsNormalization() {
    Path outputPath = Paths.get(datasetDirectory, "tweets", "json");
    try {
      if (!Files.exists(outputPath)) {
        return true;
      }
      try (Stream<Path> files = Files.list(outputPath)) {
        return files.noneMatch(p -> p.toString().endsWith(".json"));
      }
    } catch (IOException e) {
      logger.error("Error checking normalization status: {}", e.getMessage());
      return true;
    }
  }

}

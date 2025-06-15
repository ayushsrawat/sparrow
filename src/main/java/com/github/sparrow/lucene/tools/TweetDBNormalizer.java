package com.github.sparrow.lucene.tools;

import com.github.sparrow.exception.IndexingException;
import com.github.sparrow.lucene.entity.Tweet;
import com.github.sparrow.repository.TweetRepository;
import com.github.sparrow.util.ParseUtil;
import com.github.sparrow.util.TweetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * reads tweets from csv and stores them in database
 */
@Service("tweetDbNormalizer")
public class TweetDBNormalizer extends AbstractTweetNormalizer {

  private static final Logger logger = LoggerFactory.getLogger(TweetDBNormalizer.class);

  private final TweetBatchSaver tweetBatchSaver;
  private final TweetRepository tweetRepository;
  private final TweetUtil tweetUtil;

  public TweetDBNormalizer(
    TweetBatchSaver tweetBatchSaver, TweetRepository tweetRepository,
    TweetUtil tweetUtil, ParseUtil parseUtil
  ) {
    super(parseUtil);
    this.tweetBatchSaver = tweetBatchSaver;
    this.tweetRepository = tweetRepository;
    this.tweetUtil = tweetUtil;
  }

  @Override
  public void normalizeCsv() throws IndexingException {
    try {
      tweetRepository.deleteAllInBulk();
      int total = 0;
      total += normalizeTwitterDataset(tweetBatchSaver::saveAllInBatch, twitterDatasetV1);
      total += normalizeTwitterDataset(tweetBatchSaver::saveAllInBatch, twitterDatasetV2);
      total += normalizeTwitterDataset(tweetBatchSaver::saveAllInBatch, twitterDatasetV3);
      logger.info("Successfully inserted {} tweets into the database", total);
    } catch (Exception e) {
      final String msg = "Error normalizing DB";
      logger.error(msg, e);
      throw new IndexingException(msg, e);
    }
  }

  @Override
  public List<Tweet> getNormalizedTweets() {
    return tweetRepository.findAll().stream().map(tweetUtil::toTweet).toList();
  }

  @Override
  public boolean needsNormalization() {
    return tweetRepository.count() <= 100_000;
  }

}

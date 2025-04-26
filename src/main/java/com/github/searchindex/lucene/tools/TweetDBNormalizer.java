package com.github.searchindex.lucene.tools;

import com.github.searchindex.lucene.entry.Tweet;
import com.github.searchindex.repository.TweetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
  public void normalizeCsv() {
    List<Tweet> tweets = new ArrayList<>(); // only normalize if it isn't in the database!!
    tweets.addAll(normalizeTwitterV1Dataset()); // run these three in parallel
    tweets.addAll(normalizeTwitterV2Dataset());
    tweets.addAll(normalizeTwitterV3Dataset());

    int insertedCount = tweetBatchSaver.saveAllInBatch(tweets);
    logger.info("Successfully inserted {} tweets into the database", insertedCount);
  }

  @Override
  public List<Tweet> getNormalizedTweets() {
    return tweetRepository.findAll().stream().map(tweetUtil::toTweet).toList();
  }

}

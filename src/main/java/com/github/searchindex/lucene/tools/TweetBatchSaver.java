package com.github.searchindex.lucene.tools;

import com.github.searchindex.lucene.entry.Tweet;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TweetBatchSaver {

  private static final Logger logger = LoggerFactory.getLogger(TweetBatchSaver.class);
  private final EntityManager entityManager;
  private final TweetUtil tweetUtil;

  public TweetBatchSaver(EntityManager entityManager, TweetUtil tweetUtil) {
    this.entityManager = entityManager;
    this.tweetUtil = tweetUtil;
  }

  @Transactional
  public int saveAllInBatch(List<Tweet> tweets) {
    long start = System.currentTimeMillis();
    int batchSize = 2000;
    for (int i = 0; i < tweets.size(); i++) {
      entityManager.persist(tweetUtil.toTweetData(tweets.get(i)));

      if (i > 0 && i % batchSize == 0) {
        entityManager.flush();
        entityManager.clear();
        logger.info("Flushed and cleared batch at {}", i);
      }
    }
    entityManager.flush();
    entityManager.clear();

    long end = System.currentTimeMillis();
    logger.info("Batch insert took {} ms for {} tweets", (end - start), tweets.size());
    return tweets.size();
  }

}

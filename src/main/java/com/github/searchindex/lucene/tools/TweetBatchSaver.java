package com.github.searchindex.lucene.tools;

import com.github.searchindex.lucene.entry.Tweet;
import com.github.searchindex.util.TweetUtil;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class TweetBatchSaver {

  private static final Logger logger = LoggerFactory.getLogger(TweetBatchSaver.class);
  private static final Integer TWEET_BATCH_SIZE = 2500;
  private final EntityManager entityManager;
  private final TweetUtil tweetUtil;

  @Transactional
  public void saveAllInBatch(List<Tweet> tweets, String ignore) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < tweets.size(); i++) {
      entityManager.persist(tweetUtil.toTweetData(tweets.get(i)));

      if (i > 0 && (i + 1) % TWEET_BATCH_SIZE == 0) {
        entityManager.flush();
        entityManager.clear();
        logger.info("Flushed and cleared batch at {}", i);
      }
    }
    entityManager.flush();
    entityManager.clear();

    long end = System.currentTimeMillis();
    logger.info("Batch insert took {} ms for {} tweets", (end - start), tweets.size());
  }

}

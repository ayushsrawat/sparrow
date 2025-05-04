package com.github.sparrow.lucene;

import com.github.sparrow.exception.IndexingException;
import com.github.sparrow.lucene.entry.Tweet;

import java.util.List;

/**
 * Normalization is a process of parsing source data(.csv files) and storing them in containers(db/json-files)
 */
public interface TweetNormalizer {
  /**
   * figure out if data already normalized or not
   */
  boolean needsNormalization();

  /**
   * extract tweets from .csv file to manageable containers
   */
  void normalizeCsv() throws IndexingException;

  /**
   * read tweets after they have been normalized
   */
  List<Tweet> getNormalizedTweets();

}

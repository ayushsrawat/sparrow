package com.github.searchindex.lucene;

import com.github.searchindex.lucene.entry.Tweet;

import java.util.List;

public interface TweetNormalizer {

  /**
   * extract tweets from .csv file to manageable containers
   */
  void normalizeCsv();

  /**
   * read tweets after they have been normalized
   */
  List<Tweet> getNormalizedTweets();
}

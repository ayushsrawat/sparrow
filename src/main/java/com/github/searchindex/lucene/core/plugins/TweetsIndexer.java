package com.github.searchindex.lucene.core.plugins;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.Indexer;
import com.github.searchindex.lucene.core.entry.Tweet;
import com.github.searchindex.lucene.core.tools.TweetNormalizer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TweetsIndexer implements Indexer<Tweet> {

  private static final Logger logger = LoggerFactory.getLogger(TweetsIndexer.class);
  private final TweetNormalizer tweetNormalizer;

  @Override
  public IndexType getIndexType() {
    return IndexType.TWEETS;
  }

  @Override
  public void index(IndexContext context) {
    tweetNormalizer.normalizeCsv();
  }

  @Override
  public List<Tweet> search(IndexContext context, String query) {
    return List.of();
  }

}
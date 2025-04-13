package com.github.searchindex.service.impl;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexContextFactory;
import com.github.searchindex.lucene.core.IndexMode;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.entry.SearchQuery;
import com.github.searchindex.lucene.core.entry.Tweet;
import com.github.searchindex.lucene.core.plugins.TweetsIndexer;
import com.github.searchindex.service.TwitterService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TwitterServiceImpl implements TwitterService {

  private final IndexContextFactory contextFactory;
  private final TweetsIndexer tweetsIndexer;

  @Override
  public List<Tweet> search(String query) {
    try {
      IndexContext context = contextFactory.createIndexContext(IndexType.TWEETS, IndexMode.SEARCHING);
      return tweetsIndexer.search(context, SearchQuery.builder().query(query).build());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

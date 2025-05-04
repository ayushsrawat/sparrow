package com.github.sparrow.service.impl;

import com.github.sparrow.lucene.IndexContext;
import com.github.sparrow.lucene.IndexContextFactory;
import com.github.sparrow.lucene.IndexMode;
import com.github.sparrow.lucene.IndexType;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.entry.Tweet;
import com.github.sparrow.lucene.plugins.TweetsIndexer;
import com.github.sparrow.service.TwitterService;
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
  public List<Tweet> search(String query, String username) {
    try (IndexContext context = contextFactory.createIndexContext(IndexType.TWEETS, IndexMode.SEARCHING)) {
      return tweetsIndexer.search(context, SearchQuery.builder().query(query).username(username).build());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public List<Tweet> searchByUsername(String username) {
    try (IndexContext context = contextFactory.createIndexContext(IndexType.TWEETS, IndexMode.SEARCHING)) {
      return tweetsIndexer.searchByUsername(context, username);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public Integer getAllIndexedTweets() {
    try (IndexContext context = contextFactory.createIndexContext(IndexType.TWEETS, IndexMode.SEARCHING)) {
      return tweetsIndexer.getIndexedTweets(context).size();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

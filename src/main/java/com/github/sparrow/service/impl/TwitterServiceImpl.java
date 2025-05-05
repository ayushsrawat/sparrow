package com.github.sparrow.service.impl;

import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.entry.Tweet;
import com.github.sparrow.lucene.engines.TweetsEngine;
import com.github.sparrow.service.TwitterService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TwitterServiceImpl implements TwitterService {

  private final LuceneContextFactory contextFactory;
  private final TweetsEngine tweetsEngine;

  @Override
  public List<Tweet> search(String query, String username) {
    try (LuceneContext context = contextFactory.createLuceneContext(EngineType.TWEETS, LuceneMode.SEARCHING)) {
      return tweetsEngine.search(context, SearchQuery.builder().query(query).username(username).build());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public List<Tweet> searchByUsername(String username) {
    try (LuceneContext context = contextFactory.createLuceneContext(EngineType.TWEETS, LuceneMode.SEARCHING)) {
      return tweetsEngine.searchByUsername(context, username);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public Integer getAllIndexedTweets() {
    try (LuceneContext context = contextFactory.createLuceneContext(EngineType.TWEETS, LuceneMode.SEARCHING)) {
      return tweetsEngine.getIndexedTweets(context).size();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

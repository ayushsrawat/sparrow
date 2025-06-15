package com.github.sparrow.service.impl;

import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.entity.SearchHit;
import com.github.sparrow.lucene.entity.SearchQuery;
import com.github.sparrow.lucene.entity.Tweet;
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
  public List<SearchHit<Tweet>> search(String query, Integer topN, String username, Boolean stem) {
    try (LuceneContext context = contextFactory.createLuceneContext(EngineType.TWEETS, LuceneMode.SEARCHING, stem)) {
      return tweetsEngine.search(context,
        SearchQuery.builder()
          .query(query)
          .username(username)
          .topN(topN)
          .build());
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

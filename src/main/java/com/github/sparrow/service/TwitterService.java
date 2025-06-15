package com.github.sparrow.service;

import com.github.sparrow.lucene.entity.SearchHit;
import com.github.sparrow.lucene.entity.Tweet;

import java.util.List;

public interface TwitterService {

  List<SearchHit<Tweet>> search(String query, String username, Boolean stem);

  List<Tweet> searchByUsername(String username);

  Integer getAllIndexedTweets();

}

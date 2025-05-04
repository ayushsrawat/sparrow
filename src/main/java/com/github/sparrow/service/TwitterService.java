package com.github.sparrow.service;

import com.github.sparrow.lucene.entry.Tweet;

import java.util.List;

public interface TwitterService {

  List<Tweet> search(String query, String username);

  List<Tweet> searchByUsername(String username);

  Integer getAllIndexedTweets();

}

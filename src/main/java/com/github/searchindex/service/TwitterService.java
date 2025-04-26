package com.github.searchindex.service;

import com.github.searchindex.lucene.entry.Tweet;

import java.util.List;

public interface TwitterService {

  List<Tweet> search(String query);

  List<Tweet> searchByUsername(String username);

  Integer getAllIndexedTweets();

}

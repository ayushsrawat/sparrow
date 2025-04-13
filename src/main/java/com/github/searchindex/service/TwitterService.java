package com.github.searchindex.service;

import com.github.searchindex.lucene.core.entry.Tweet;

import java.util.List;

public interface TwitterService {

  List<Tweet> search(String query);

}

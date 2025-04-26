package com.github.searchindex.controller;

import com.github.searchindex.lucene.entry.DictionaryEntry;
import com.github.searchindex.lucene.entry.Tweet;
import com.github.searchindex.service.DictionaryService;
import com.github.searchindex.service.TwitterService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/search")
@RequiredArgsConstructor
public class SearchController {

  private final DictionaryService dictionaryService;
  private final TwitterService twitterService;

  @GetMapping("/dictionary")
  public ResponseEntity<List<DictionaryEntry>> dictionarySearch(@RequestParam("q") String query) {
    return ResponseEntity.ok(dictionaryService.search(query));
  }

  @GetMapping("/twitter")
  public ResponseEntity<List<Tweet>> tweetSearch(@RequestParam("q") String query) {
    return ResponseEntity.ok(twitterService.search(query));
  }

  @GetMapping("/user/{username}")
  public ResponseEntity<List<Tweet>> searchByUsername(@PathVariable("username") String username) {
    return ResponseEntity.ok(twitterService.searchByUsername(username));
  }

  @GetMapping("/twitter/count")
  public ResponseEntity<Integer> indexedTweetsCount() {
    return ResponseEntity.ok(twitterService.getAllIndexedTweets());
  }

}

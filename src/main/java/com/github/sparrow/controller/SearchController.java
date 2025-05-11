package com.github.sparrow.controller;

import com.github.sparrow.dto.ArticleSearchResponse;
import com.github.sparrow.lucene.entry.DictionaryEntry;
import com.github.sparrow.lucene.entry.Tweet;
import com.github.sparrow.service.ArticleService;
import com.github.sparrow.service.DictionaryService;
import com.github.sparrow.service.TwitterService;
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

  private final ArticleService articleService;
  private final DictionaryService dictionaryService;
  private final TwitterService twitterService;

  @GetMapping("/dictionary")
  public ResponseEntity<List<DictionaryEntry>> dictionarySearch(@RequestParam("q") String query) {
    return ResponseEntity.ok(dictionaryService.search(query));
  }

  @GetMapping("/twitter")
  public ResponseEntity<List<Tweet>> tweetSearch(
    @RequestParam(value = "q") String query,
    @RequestParam(value = "from", required = false) String username,
    @RequestParam(value = "stem", required = false, defaultValue = "true") boolean stem
  ) {
    return ResponseEntity.ok(twitterService.search(query, username, stem));
  }

  @GetMapping("/user/{username}")
  public ResponseEntity<List<Tweet>> searchByUsername(@PathVariable("username") String username) {
    return ResponseEntity.ok(twitterService.searchByUsername(username));
  }

  @GetMapping("/twitter/count")
  public ResponseEntity<Integer> indexedTweetsCount() {
    return ResponseEntity.ok(twitterService.getAllIndexedTweets());
  }

  @GetMapping("/article")
  public ResponseEntity<List<ArticleSearchResponse>> articleSearch(
    @RequestParam(value = "q") String query,
    @RequestParam(value = "stem", required = false, defaultValue = "true") boolean stem
  ) {
    return ResponseEntity.ok(articleService.search(query, stem));
  }

  @GetMapping("/article/tokens")
  public ResponseEntity<List<String>> articleIndexedTokens() {
    return ResponseEntity.ok(articleService.getIndexedTokens());
  }

}

package com.github.searchindex.lucene.core.entry;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
public class Tweet {

  private Long    tweetId;
  private String  username;
  private String  fullName;
  private String  tweet;
  private String  url;
  private Integer views;
  private Integer likes;
  private Integer retweets;
  private LocalDateTime tweetDate;

  @Override
  public String toString() {
    return "TweetId: " + tweetId +
      "\nUsername: " + username +
      "\nTweet: " + tweet +
      "\nDate: " + tweetDate +
      "\nUrl: " + url;
  }

}

package com.github.sparrow.lucene.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Tweet {

  private Long    tweetId;
  private String  username;
  private String  fullName;
  private String  tweet;
  private String  url;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private Integer views;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private Integer likes;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private Integer retweets;
  private LocalDateTime tweetDate;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (tweetId != null) sb.append("TweetId: ").append(tweetId).append("\n");
    if (username != null && !username.isBlank()) sb.append("Username: ").append(username).append("\n");
    if (fullName != null && !fullName.isBlank()) sb.append("Full Name: ").append(fullName).append("\n");
    if (tweet != null && !tweet.isBlank()) sb.append("Tweet: ").append(tweet).append("\n");
    if (tweetDate != null) sb.append("Date: ").append(tweetDate).append("\n");
    if (url != null && !url.isBlank()) sb.append("URL: ").append(url).append("\n");
    if (views != null) sb.append("Views: ").append(views).append("\n");
    if (likes != null) sb.append("Likes: ").append(likes).append("\n");
    if (retweets != null) sb.append("Retweets: ").append(retweets).append("\n");
    return sb.toString().strip();
  }

}

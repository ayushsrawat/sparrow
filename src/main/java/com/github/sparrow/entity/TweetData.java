package com.github.sparrow.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "tweet_data")
@Getter
@Builder
@AllArgsConstructor
public class TweetData {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Integer tweetId;

  @Column(name = "xid")
  private Long xId;

  @Column(nullable = false)
  private String username;

  private String fullName;

  @Column(nullable = false)
  private String tweet;

  private String url;

  @Column(name = "t_views")
  private Integer views;

  @Column(name = "t_likes")
  private Integer likes;

  @Column(name = "t_retweets")
  private Integer retweets;

  @Column(name = "created_at")
  private LocalDateTime creationAt;

  public TweetData() {/* for builder*/}
}

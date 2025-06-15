package com.github.sparrow.util;

import com.github.sparrow.entity.TweetData;
import com.github.sparrow.lucene.entity.Tweet;
import org.springframework.stereotype.Component;

@Component
public class TweetUtil {

  public TweetData toTweetData(Tweet t) {
    return TweetData.builder()
      .xId(t.getTweetId())
      .username(t.getUsername())
      .fullName(t.getFullName())
      .tweet(t.getTweet())
      .url(t.getUrl())
      .likes(t.getLikes() == null ? 0 : t.getLikes())
      .views(t.getViews() == null ? 0 : t.getViews())
      .retweets(t.getRetweets() == null ? 0 : t.getRetweets())
      .creationAt(t.getTweetDate())
      .build();
  }

  public Tweet toTweet(TweetData t) {
    return Tweet.builder()
      .tweetId(t.getXId())
      .username(t.getUsername())
      .fullName(t.getFullName())
      .tweet(t.getTweet())
      .likes(t.getLikes() == null ? 0 : t.getLikes())
      .views(t.getViews() == null ? 0 : t.getViews())
      .retweets(t.getRetweets() == null ? 0 : t.getRetweets())
      .tweetDate(t.getCreationAt())
      .build();
  }

}

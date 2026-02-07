package com.github.sparrow.payload.response;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
public class ArticleStatusResponse {

  private String url;
  private String title;
  private Integer contentLength;
  private LocalDateTime crawledAt;

}

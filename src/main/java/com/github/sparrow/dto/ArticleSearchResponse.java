package com.github.sparrow.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ArticleSearchResponse {

  private String url;
  private String title;
  private String content;
  private String contentHash;

}

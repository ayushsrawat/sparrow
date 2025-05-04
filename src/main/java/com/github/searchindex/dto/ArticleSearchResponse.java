package com.github.searchindex.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ArticleSearchResponse {

  private String url;
  private String title;
  private String contentHash;

}

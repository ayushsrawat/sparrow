package com.github.sparrow.service;

import com.github.sparrow.dto.ArticleSearchResponse;

import java.util.List;

public interface ArticleService {

  List<ArticleSearchResponse> search(String query);

  List<String> getIndexedTokens();

}

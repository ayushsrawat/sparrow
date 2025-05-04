package com.github.searchindex.service;

import com.github.searchindex.dto.ArticleSearchResponse;

import java.util.List;

public interface ArticleService {

  List<ArticleSearchResponse> search(String query);

  List<String> getIndexedTokens();

}

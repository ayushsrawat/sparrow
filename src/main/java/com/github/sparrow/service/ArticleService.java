package com.github.sparrow.service;

import com.github.sparrow.payload.response.ArticleSearchResponse;
import com.github.sparrow.lucene.entity.SearchHit;
import com.github.sparrow.payload.response.ArticleStatusResponse;

import java.util.List;

public interface ArticleService {

  List<SearchHit<ArticleSearchResponse>> search(String query, Integer topN, Boolean stem);

  List<String> getIndexedTokens();

  List<ArticleStatusResponse> getStatus();

}

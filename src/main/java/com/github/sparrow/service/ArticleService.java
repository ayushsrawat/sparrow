package com.github.sparrow.service;

import com.github.sparrow.dto.ArticleSearchResponse;
import com.github.sparrow.lucene.entity.SearchHit;

import java.util.List;

public interface ArticleService {

  List<SearchHit<ArticleSearchResponse>> search(String query, Boolean stem);

  List<String> getIndexedTokens();

}

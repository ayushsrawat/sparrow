package com.github.sparrow.service.impl;

import com.github.sparrow.dto.ArticleSearchResponse;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
import com.github.sparrow.lucene.engines.ArticlesEngine;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.service.ArticleService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ArticleServiceImpl implements ArticleService {

  private final ArticlesEngine articlesEngine;
  private final LuceneContextFactory luceneContextFactory;

  @Override
  public List<ArticleSearchResponse> search(String query) {
    try (LuceneContext context = luceneContextFactory.createLuceneContext(EngineType.ARTICLES, LuceneMode.SEARCHING)) {
      return articlesEngine.search(context, SearchQuery.builder().query(query).build());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public List<String> getIndexedTokens() {
    try (LuceneContext context = luceneContextFactory.createLuceneContext(EngineType.ARTICLES, LuceneMode.SEARCHING)) {
      return articlesEngine.getIndexedTokens(context, ArticlesEngine.IndexField.CONTENT);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

package com.github.sparrow.service.impl;

import com.github.sparrow.dto.ArticleSearchResponse;
import com.github.sparrow.entity.CrawledPage;
import com.github.sparrow.lucene.IndexContext;
import com.github.sparrow.lucene.IndexContextFactory;
import com.github.sparrow.lucene.IndexMode;
import com.github.sparrow.lucene.IndexType;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.plugins.ArticlesIndexer;
import com.github.sparrow.service.ArticleService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ArticleServiceImpl implements ArticleService {

  private final ArticlesIndexer articlesIndexer;
  private final IndexContextFactory indexContextFactory;

  @Override
  public List<ArticleSearchResponse> search(String query) {
    try (IndexContext context = indexContextFactory.createIndexContext(IndexType.ARTICLES, IndexMode.SEARCHING)) {
      List<CrawledPage> crawledPages = articlesIndexer.search(context, SearchQuery.builder().query(query).build());
      List<ArticleSearchResponse> searchResponses = new ArrayList<>();
      for (CrawledPage p : crawledPages) {
        searchResponses.add(ArticleSearchResponse
          .builder()
          .url(p.getUrl())
          .title(p.getTitle())
          .contentHash(p.getContentHash())
          .build());
      }
      return searchResponses;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

  @Override
  public List<String> getIndexedTokens() {
    try (IndexContext context = indexContextFactory.createIndexContext(IndexType.ARTICLES, IndexMode.SEARCHING)) {
      return articlesIndexer.getIndexedTokens(context, ArticlesIndexer.IndexField.CONTENT);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

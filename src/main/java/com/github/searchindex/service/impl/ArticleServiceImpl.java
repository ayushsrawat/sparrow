package com.github.searchindex.service.impl;

import com.github.searchindex.dto.ArticleSearchResponse;
import com.github.searchindex.entity.CrawledPage;
import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexContextFactory;
import com.github.searchindex.lucene.IndexMode;
import com.github.searchindex.lucene.IndexType;
import com.github.searchindex.lucene.entry.SearchQuery;
import com.github.searchindex.lucene.plugins.ArticlesIndexer;
import com.github.searchindex.service.ArticleService;
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
      return articlesIndexer.getIndexedTokens(context);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage());
    }
  }

}

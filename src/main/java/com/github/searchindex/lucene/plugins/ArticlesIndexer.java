package com.github.searchindex.lucene.plugins;

import com.github.searchindex.entity.CrawledPage;
import com.github.searchindex.exception.IndexingException;
import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexType;
import com.github.searchindex.lucene.Indexer;
import com.github.searchindex.lucene.entry.SearchQuery;
import com.github.searchindex.util.DateUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ArticlesIndexer implements Indexer<CrawledPage> {

  private static final Logger logger = LoggerFactory.getLogger(ArticlesIndexer.class);

  private final DateUtil dateUtil;

  @Override
  public IndexType getIndexType() {
    return IndexType.ARTICLES;
  }

  @Getter
  private enum IndexField {
    ID("id"),
    URL("url"),
    TITLE("title"),
    CONTENT("content"),
    CRAWLED_AT("crawled-at"),
    CONTENT_HASH("content-hash");
    private final String name;

    IndexField(String name) {
      this.name = name;
    }
  }

  @Override
  public void index(IndexContext context) throws IndexingException {
    /// articles are crawled by spiders one by one >> no operation
  }

  @Override
  public void indexDocument(IndexContext context, CrawledPage article) throws IOException {
    logger.info("Indexing article: {}", article.getUrl());
    Document document = new Document();
    document.add(new IntField(IndexField.ID.getName(), article.getId(), Field.Store.YES));
    document.add(new StringField(IndexField.URL.getName(), article.getUrl(), Field.Store.YES));
    document.add(new StringField(IndexField.TITLE.getName(), article.getTitle(), Field.Store.YES));
    document.add(new TextField(IndexField.CONTENT.getName(), article.getContent(), Field.Store.NO));
    document.add(new LongPoint(IndexField.CRAWLED_AT.getName(), dateUtil.convertToLong(article.getLastCrawledAt())));
    document.add(new StringField(IndexField.CONTENT_HASH.getName(), article.getContentHash(), Field.Store.YES));
    context.getWriter().addDocument(document);
    context.getWriter().commit();
  }

  @Override
  public List<CrawledPage> search(IndexContext context, SearchQuery searchQuery) {
    return List.of();
  }

}

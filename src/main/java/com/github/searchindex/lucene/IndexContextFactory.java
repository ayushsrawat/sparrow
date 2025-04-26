package com.github.searchindex.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
@PropertySource("classpath:index.properties")
public class IndexContextFactory {

  private static final Logger logger = LoggerFactory.getLogger(IndexContextFactory.class);

  @Value("${index.path}")
  private String indexPath;

  /**
   * Context to save the lucene indexes in ~/lucene/search-index/{index-name}
   */
  public IndexContext createIndexContext(IndexType indexType, IndexMode indexMode) throws IOException {
    logger.info("Creating Lucene context for {} in {} mode", indexType.getName(), indexMode);
    final String luceneIndexPath = createLuceneIndexDir();
    File indexDir = new File(luceneIndexPath + indexType.getName());
    if (!indexDir.exists() && !indexDir.mkdirs()) {
      logger.error("Error creating index directory {}", luceneIndexPath);
      throw new IOException();
    }
    Directory luceneDirectory = FSDirectory.open(indexDir.toPath());
    logger.info("Lucene directory : {}", luceneDirectory);

    Analyzer analyzer = new StandardAnalyzer();
    if (IndexMode.INDEXING.equals(indexMode)) {
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      IndexWriter writer = new IndexWriter(luceneDirectory, config);
      return IndexContext.builder()
        .directory(luceneDirectory)
        .writer(writer)
        .analyzer(analyzer)
        .build();
    }

    //do not need IndexWriter while searching the indexes; it throws write.lock
    return IndexContext.builder()
      .directory(luceneDirectory)
      .analyzer(analyzer)
      .build();
  }

  private String createLuceneIndexDir() {
    return System.getProperty("user.home") + indexPath;
  }

}

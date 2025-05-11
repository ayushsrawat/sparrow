package com.github.sparrow.lucene;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.analysis.Analyzer;
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
@PropertySource("classpath:sparrow.properties")
@RequiredArgsConstructor
public class LuceneContextFactory {

  private static final Logger logger = LoggerFactory.getLogger(LuceneContextFactory.class);

  private final AnalyzerProvider analyzerProvider;

  @Value("${index.path}")
  private String indexPath;

  /**
   * Context to save the lucene indexes in ~/lucene/search-index/{index-name}
   */
  public LuceneContext createLuceneContext(EngineType engineType, LuceneMode luceneMode) throws IOException {
    return createLuceneContext(engineType, luceneMode, true);
  }

  public LuceneContext createLuceneContext(EngineType engineType, LuceneMode luceneMode, Boolean stemming) throws IOException {
    logger.info("Creating Lucene context for {} in {} mode", engineType.getName(), luceneMode);
    final String luceneIndexPath = createLuceneIndexDir();
    File indexDir = new File(luceneIndexPath + engineType.getName());
    if (!indexDir.exists() && !indexDir.mkdirs()) {
      logger.error("Error creating index directory {}", luceneIndexPath);
      throw new IOException();
    }
    Directory luceneDirectory = FSDirectory.open(indexDir.toPath());
    logger.info("Lucene directory : {}", luceneDirectory);

    Analyzer analyzer = analyzerProvider.getAnalyzer(engineType, luceneMode, stemming);
    if (LuceneMode.INDEXING.equals(luceneMode)) {
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
      IndexWriter writer = new IndexWriter(luceneDirectory, config);
      return LuceneContext.builder()
        .directory(luceneDirectory)
        .writer(writer)
        .analyzer(analyzer)
        .build();
    }

    //do not need IndexWriter while searching the indexes; it throws write.lock
    return LuceneContext.builder()
      .directory(luceneDirectory)
      .analyzer(analyzer)
      .build();
  }

  private String createLuceneIndexDir() {
    return System.getProperty("user.home") + indexPath;
  }

}

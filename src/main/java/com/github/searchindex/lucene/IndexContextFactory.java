package com.github.searchindex.lucene;

import com.github.searchindex.lucene.core.IndexMode;
import com.github.searchindex.lucene.core.IndexType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
public class IndexContextFactory {

  private static final Logger logger = LoggerFactory.getLogger(IndexContextFactory.class);
  private static final String LUCENE_INDEX_DIR = System.getProperty("user.home") + "/lucene/search-index/";

  /**
   * Context to save the lucene indexes in ~/lucene/search-index/{index-name}
   */
  public IndexContext createIndexContext(IndexType indexType, IndexMode indexMode) throws IOException {
    File indexDir = new File(LUCENE_INDEX_DIR + indexType.getName());
    if (!indexDir.exists() && !indexDir.mkdirs()) {
      logger.error("Error creating index directory {}", LUCENE_INDEX_DIR);
      throw new IOException();
    }
    Directory luceneDirectory = FSDirectory.open(indexDir.toPath());
    logger.info("Lucene context : {}", luceneDirectory);

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

}

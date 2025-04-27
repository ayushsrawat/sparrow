package com.github.searchindex.lucene;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Start indexing all the Indexers as this bean initializes
 */
@Component
@RequiredArgsConstructor
@PropertySource("classpath:index.properties")
public class IndexListener {

  private final Logger logger = LoggerFactory.getLogger(IndexListener.class);
  private final List<Indexer<?>> indexers;
  private final IndexContextFactory contextFactory;
  private final ExecutorService executorService;

  @Value("${indexing.parallel}")
  private boolean parallelExecution;

  @PostConstruct
  public void initIndex() {
    for (Indexer<?> indexer : indexers) {
      if (parallelExecution) {
        CompletableFuture.runAsync(() -> runIndexer(indexer), executorService);
      } else {
        runIndexer(indexer);
      }
    }
  }

  private void runIndexer(Indexer<?> indexer) {
    IndexType indexType = indexer.getIndexType();
    logger.info("Initializing Indexer : {}", indexType.getName());
    try {
      IndexContext context = contextFactory.createIndexContext(indexType, IndexMode.INDEXING);
      logger.info("Created index context : {}", context);
      indexer.index(context);
    } catch (IOException ioe) {
      logger.error("Error indexing {}: {}", indexType.getName(), ioe.getMessage(), ioe);
    }
  }

}

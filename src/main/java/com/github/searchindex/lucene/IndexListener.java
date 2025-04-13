package com.github.searchindex.lucene;

import com.github.searchindex.lucene.core.IndexMode;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.Indexer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Start indexing all the Indexers as this bean initializes
 */
@Component
@AllArgsConstructor
public class IndexListener {

  private final Logger logger = LoggerFactory.getLogger(IndexListener.class);
  private final List<Indexer<?>> indexers;
  private final IndexContextFactory contextFactory;
  private final ExecutorService executorService;

  @PostConstruct
  public void initIndex() {
    for (Indexer<?> indexer : indexers) {
      CompletableFuture.runAsync(() -> {
        IndexType indexType = indexer.getIndexType();
        logger.info("Initializing Indexer : {}", indexType.getName());
        try {
          IndexContext context = contextFactory.createIndexContext(indexType, IndexMode.INDEXING);
          logger.info("Created index context : {}", context);
          indexer.index(context);
        } catch (IOException ioe) {
          logger.error("Error indexing {}: {}", indexType.getName(), ioe.getMessage(), ioe);
        }
      }, executorService);
    }
  }

  @PreDestroy
  public void cleanup() {
    executorService.shutdown();
  }

}

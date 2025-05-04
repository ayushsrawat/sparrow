package com.github.sparrow.lucene.core.plugin;

import com.github.sparrow.lucene.IndexContext;
import com.github.sparrow.lucene.IndexContextFactory;
import com.github.sparrow.lucene.IndexMode;
import com.github.sparrow.lucene.IndexType;
import com.github.sparrow.lucene.entry.DictionaryEntry;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.plugins.DictionaryIndexer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DictionaryIndexerTest {

  private static final Logger logger = LoggerFactory.getLogger(DictionaryIndexerTest.class);

  @Test
  public void searchTest() throws IOException {
    IndexContextFactory factory = new IndexContextFactory();
    IndexContext context = factory.createIndexContext(IndexType.DICTIONARY, IndexMode.SEARCHING);
    DictionaryIndexer dictionaryIndexer = new DictionaryIndexer();
    List<DictionaryEntry> search = dictionaryIndexer.search(context, SearchQuery.builder().query("listen").build());
    logger.info("Searched result  : {}", search);
  }

}

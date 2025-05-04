package com.github.sparrow.service.impl;

import com.github.sparrow.lucene.IndexContext;
import com.github.sparrow.lucene.IndexContextFactory;
import com.github.sparrow.lucene.IndexMode;
import com.github.sparrow.lucene.IndexType;
import com.github.sparrow.lucene.entry.DictionaryEntry;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.plugins.DictionaryIndexer;
import com.github.sparrow.service.DictionaryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DictionaryServiceImpl implements DictionaryService {

  private final IndexContextFactory indexContextFactory;
  private final DictionaryIndexer dictionaryIndexer;

  @Override
  public List<DictionaryEntry> search(String query) {
    try (IndexContext context = indexContextFactory.createIndexContext(IndexType.DICTIONARY, IndexMode.SEARCHING)) {
      return dictionaryIndexer.search(context, SearchQuery.builder().query(query).build());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

}

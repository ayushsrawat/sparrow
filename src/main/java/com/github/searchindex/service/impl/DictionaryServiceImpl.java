package com.github.searchindex.service.impl;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexContextFactory;
import com.github.searchindex.lucene.core.IndexMode;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.entry.DictionaryEntry;
import com.github.searchindex.lucene.core.plugins.DictionaryIndexer;
import com.github.searchindex.service.DictionaryService;
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
    try {
      IndexContext context = indexContextFactory.createIndexContext(IndexType.DICTIONARY, IndexMode.SEARCHING);
      return dictionaryIndexer.search(context, query);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

}

package com.github.sparrow.service.impl;

import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.entry.DictionaryEntry;
import com.github.sparrow.lucene.entry.SearchQuery;
import com.github.sparrow.lucene.engines.DictionaryEngine;
import com.github.sparrow.service.DictionaryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DictionaryServiceImpl implements DictionaryService {

  private final LuceneContextFactory luceneContextFactory;
  private final DictionaryEngine dictionaryEngine;

  @Override
  public List<DictionaryEntry> search(String query) {
    try (LuceneContext context = luceneContextFactory.createLuceneContext(EngineType.DICTIONARY, LuceneMode.SEARCHING)) {
      return dictionaryEngine.search(context, SearchQuery.builder().query(query).build());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

}

package com.github.sparrow.service;

import com.github.sparrow.lucene.entity.DictionaryEntry;
import com.github.sparrow.lucene.entity.SearchHit;

import java.util.List;

public interface DictionaryService {

  List<SearchHit<DictionaryEntry>> search(String query);

}

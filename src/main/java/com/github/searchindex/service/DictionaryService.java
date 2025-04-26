package com.github.searchindex.service;

import com.github.searchindex.lucene.entry.DictionaryEntry;

import java.util.List;

public interface DictionaryService {

  List<DictionaryEntry> search(String query);

}

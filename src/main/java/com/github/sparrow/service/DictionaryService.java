package com.github.sparrow.service;

import com.github.sparrow.lucene.entry.DictionaryEntry;

import java.util.List;

public interface DictionaryService {

  List<DictionaryEntry> search(String query);

}

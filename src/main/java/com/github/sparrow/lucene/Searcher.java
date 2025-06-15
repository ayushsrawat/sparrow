package com.github.sparrow.lucene;

import com.github.sparrow.lucene.entity.SearchQuery;

import java.util.List;

/**
 * Search from the lucene indexes.
 */
public interface Searcher<T> {

  List<T> search(LuceneContext context, SearchQuery searchQuery);

}

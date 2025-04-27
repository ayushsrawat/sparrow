package com.github.searchindex.lucene;

import com.github.searchindex.lucene.entry.SearchQuery;

import java.util.List;

/**
 * Indexers are heart of this application!! They manage the indexing of data, from which you can query later.
 * @see <a href="https://notes.stephenholiday.com/Earlybird.pdf">Earlybird: Real-Time Search at Twitter</a>
 * @see <a href="https://web.archive.org/web/20130904073403/http://www.ibm.com/developerworks/library/wa-lucene/">Delve inside the Lucene indexing mechanism</a>
 * @see <a href="https://lucene.sourceforge.net/talks/pisa/">Doug Cutting</a>
 * @see <a href="https://stackoverflow.com/questions/2602253/how-does-lucene-index-documents">how-does-lucene-index-documents</a>
 * @see <a href="https://github.com/apache/lucene/blob/main/lucene/demo/src/java/org/apache/lucene/demo/IndexFiles.java">demo</a>
 * @see <a href="https://lucene.apache.org/core/10_2_0/demo/index.html">docs</a>
 */
public interface Indexer<T> {

  IndexType getIndexType();

  void index(IndexContext context);

  List<T> search(IndexContext context, SearchQuery searchQuery);

}

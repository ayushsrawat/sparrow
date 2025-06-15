package com.github.sparrow.lucene;

import com.github.sparrow.exception.IndexingException;

import java.io.IOException;

/**
 * Indexers are heart of this application!! They manage the indexing of data, from which you can query later.
 *
 * @see <a href="https://notes.stephenholiday.com/Earlybird.pdf">Earlybird: Real-Time Search at Twitter</a>
 * @see <a href="https://snap.stanford.edu/class/cs224w-readings/Brin98Anatomy.pdf">The anatomy of a large-scale hypertextual Web search engine</a>
 * @see <a href="https://web.archive.org/web/20130904073403/http://www.ibm.com/developerworks/library/wa-lucene/">Delve inside the Lucene indexing mechanism</a>
 * @see <a href="https://lucene.sourceforge.net/talks/pisa/">Doug Cutting</a>
 * @see <a href="https://stackoverflow.com/questions/2602253/how-does-lucene-index-documents">how-does-lucene-index-documents</a>
 * @see <a href="https://github.com/apache/lucene/blob/main/lucene/demo/src/java/org/apache/lucene/demo/IndexFiles.java">demo</a>
 * @see <a href="https://lucene.apache.org/core/10_2_0/demo/index.html">docs</a>
 */
public interface Indexer<T> {

  EngineType getEngineType();

  void index(LuceneContext context) throws IndexingException;

  void indexDocument(LuceneContext context, T document) throws IOException;

  default boolean needsIndexing(LuceneContext context) {
    return false;
  }

}

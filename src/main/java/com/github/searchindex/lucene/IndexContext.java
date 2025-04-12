package com.github.searchindex.lucene;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

@Getter
@Setter
@Builder
public class IndexContext {

  private Directory directory;
  private IndexWriter writer;
  private Analyzer analyzer;

  @Override
  public String toString() {
    return "Index Directory : " + directory + "\n IndexWriter :" + writer + "\n Analyzer : " + analyzer;
  }

}

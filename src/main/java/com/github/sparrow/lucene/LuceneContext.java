package com.github.sparrow.lucene;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Setter
@Builder
public class LuceneContext implements Closeable {

  private Directory directory;
  private IndexWriter writer;
  private Analyzer analyzer;

  @Override
  public void close() throws IOException {
    if (analyzer != null) analyzer.close();
    if (writer != null) writer.close();
    if (directory != null) directory.close();
  }

  @Override
  public String toString() {
    return "Index Directory : " + directory + " IndexWriter :" + writer + " Analyzer : " + analyzer;
  }

}

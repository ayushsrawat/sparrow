package com.github.searchindex.lucene;

import org.apache.lucene.analysis.Analyzer;

public interface AnalyzerProvider {

  Analyzer getAnalyzer(IndexType indexType);

}

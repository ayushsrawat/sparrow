package com.github.sparrow.lucene;

import org.apache.lucene.analysis.Analyzer;

public interface AnalyzerProvider {

  Analyzer getAnalyzer(EngineType engineType, LuceneMode luceneMode, Boolean stemming);

}

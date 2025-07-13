package com.github.sparrow.analysis;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LuceneNoScoringBenchmark {
  static final int NUM_DOCS = 100_000;
  static final int NUM_MATCHING_DOCS = 10_000;

  public static void main(String[] args) throws IOException {
    Path indexPath = Files.createTempDirectory("lucene-benchmark");
    Directory directory = FSDirectory.open(indexPath);
    StandardAnalyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    IndexWriter writer = new IndexWriter(directory, config);

    // 1. Index sample documents
    for (int i = 0; i < NUM_DOCS; i++) {
      Document doc = new Document();
      String fileId = String.valueOf(i);
      doc.add(new StringField("fileId", fileId, Field.Store.YES));
      doc.add(new StoredField("content", "This is content for fileId " + fileId));
      writer.addDocument(doc);
    }
    writer.close();

    // 2. Open index
    DirectoryReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    // 3. Prepare ID set to query
    List<String> matchedIds = new ArrayList<>();
    for (int i = 10_000; i < 10_000 + NUM_MATCHING_DOCS; i++) {
      matchedIds.add(String.valueOf(i));
    }
    List<BytesRef> terms = new ArrayList<>();
    for (String id : matchedIds) terms.add(new BytesRef(id));

    Query query = new TermInSetQuery("fileId", terms);

    // 4. Scoring mode
    long startScoring = System.nanoTime();
    TopDocs topDocs = searcher.search(query, 10_000); // limit to avoid OOM
    long endScoring = System.nanoTime();

    // 5. No-scoring mode
    Query constQuery = new ConstantScoreQuery(query);
    class NoScoreCollector extends SimpleCollector {
      private LeafReaderContext context;
      private final List<Integer> hits = new ArrayList<>();

      public List<Integer> getHits() {
        return hits;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) {
        this.context = context;
      }

      @Override
      public void collect(int doc) {
        hits.add(doc + context.docBase);
      }
    }

    long startNoScoring = System.nanoTime();
    List<Integer> noScoreHits = searcher.search(constQuery, new CollectorManager<NoScoreCollector, List<Integer>>() {
      @Override
      public NoScoreCollector newCollector() {
        return new NoScoreCollector();
      }

      @Override
      public List<Integer> reduce(Collection<NoScoreCollector> noScoreCollectors) {
        List<Integer> hits = new ArrayList<>();
        for (NoScoreCollector collector : noScoreCollectors) {
          hits.addAll(collector.getHits());
        }
        return hits;
      }
    });
    long endNoScoring = System.nanoTime();

    System.out.printf("With scoring: %.2f ms for %d results%n",
      (endScoring - startScoring) / 1_000_000.0, topDocs.scoreDocs.length);

    System.out.printf("Without scoring: %.2f ms for %d results%n",
      (endNoScoring - startNoScoring) / 1_000_000.0, noScoreHits.size());

    System.out.printf("Scoring Explanation: %s", searcher.explain(query, topDocs.scoreDocs[0].doc));
    System.out.printf("Non Scoring Explanation: %s", searcher.explain(constQuery, noScoreHits.getFirst()));

    reader.close();
    directory.close();
  }
}


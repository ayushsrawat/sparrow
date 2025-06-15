package com.github.sparrow.lucene.engines;

import com.github.sparrow.exception.IndexingException;
import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.Indexer;
import com.github.sparrow.lucene.Searcher;
import com.github.sparrow.lucene.entity.DictionaryEntry;
import com.github.sparrow.lucene.entity.SearchHit;
import com.github.sparrow.lucene.entity.SearchQuery;
import lombok.Getter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
@PropertySource("classpath:sparrow.properties")
public class DictionaryEngine implements Indexer<DictionaryEntry>, Searcher<SearchHit<DictionaryEntry>> {

  private static final Logger logger = LoggerFactory.getLogger(DictionaryEngine.class);

  @Value("${word.dictionary.txt}")
  private String wordDictionaryTxt;
  @Value("${dataset.path}")
  protected String datasetDirectory;

  @Override
  public EngineType getEngineType() {
    return EngineType.DICTIONARY;
  }

  @Override
  public boolean needsIndexing(LuceneContext context) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      return reader.maxDoc() <= 0;
    } catch (IOException ioe) {
      logger.warn(ioe.getMessage());
    }
    return true;
  }

  @Getter
  private enum IndexField {
    WORD("word"),
    PARTS_OF_SPEECH("parts-of-speech"),
    MEANING("meaning"),
    SOURCE("source");
    private final String name;

    IndexField(String name) {
      this.name = name;
    }
  }

  @Override
  public void index(LuceneContext context) throws IndexingException {
    try {
      Path dataPath = Paths.get(datasetDirectory + wordDictionaryTxt);
      if (!Files.exists(dataPath)) {
        logger.error("{} dataset not found.", dataPath);
        return;
      }
      int count = 0;
      try (IndexWriter writer = context.getWriter();
           BufferedReader reader = Files.newBufferedReader(dataPath)
      ) {
        logger.info("Indexing file {}, using writer {}", dataPath, writer);
        writer.deleteAll(); //delete previously indexed data
        String line;
        while ((line = reader.readLine()) != null) {
          count++;
          String[] row = line.split("\\|");
          if (row.length < 4) {
            logger.error("Skipping malformed line : {}", line);
            continue;
          }
          logger.debug("Indexing dictionary row : {}", (Object) row);
          DictionaryEntry dictionaryEntry = DictionaryEntry.builder()
            .word(row[0])
            .partsOfSpeech(row[1])
            .meaning(row[2])
            .source(row[3])
            .build();
          indexDocument(context, dictionaryEntry);
        }
        writer.commit();
      }
      logger.info("Successfully Indexed {} words", count);
    } catch (IOException ioe) {
      logger.error(ioe.getMessage());
      throw new IndexingException("Error indexing Dictionary " + ioe.getMessage(), ioe.getCause());
    }
  }

  @Override
  public void indexDocument(LuceneContext context, DictionaryEntry dictionaryEntry) throws IOException {
    Document document = new Document();
    document.add(new TextField(IndexField.WORD.getName(), dictionaryEntry.word(), Field.Store.YES));
    document.add(new StringField(IndexField.PARTS_OF_SPEECH.getName(), dictionaryEntry.partsOfSpeech(), Field.Store.YES));
    document.add(new TextField(IndexField.MEANING.getName(), dictionaryEntry.meaning(), Field.Store.YES));
    document.add(new StringField(IndexField.SOURCE.getName(), dictionaryEntry.source(), Field.Store.YES));
    context.getWriter().addDocument(document); // updateDocument if already exists?
  }

  @Override
  public List<SearchHit<DictionaryEntry>> search(LuceneContext context, SearchQuery searchQuery) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      String ques = searchQuery.getQuery();
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(IndexField.MEANING.getName(), context.getAnalyzer());
      Query query = parser.parse(ques);
      logger.info("Searching for the query : {}, using searcher : {}", query, searcher);

      TopDocs topDocs = searcher.search(query, 10);
      List<SearchHit<DictionaryEntry>> hits = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        logger.debug("Explanation : {}", searcher.explain(query, scoreDoc.doc));
        Document document = searcher.storedFields().document(scoreDoc.doc);
        DictionaryEntry entry = DictionaryEntry.builder()
          .word(document.get(IndexField.WORD.getName()))
          .meaning(document.get(IndexField.MEANING.getName()))
          .partsOfSpeech(document.get(IndexField.PARTS_OF_SPEECH.getName()))
          .source(document.get(IndexField.SOURCE.getName()))
          .build();
        hits.add(new SearchHit<>(entry, scoreDoc.score, scoreDoc.doc));
      }
      logger.info("Searched {} words for the question {}.", hits.size(), ques);
      return hits;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

}

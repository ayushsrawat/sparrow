package com.github.searchindex.lucene.plugins;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexType;
import com.github.searchindex.lucene.Indexer;
import com.github.searchindex.lucene.entry.DictionaryEntry;
import com.github.searchindex.lucene.entry.SearchQuery;
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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Service
@PropertySource("classpath:index.properties")
public class DictionaryIndexer implements Indexer<DictionaryEntry> {

  private static final Logger logger = LoggerFactory.getLogger(DictionaryIndexer.class);

  @Value("${word.dictionary.txt}")
  private String wordDictionaryTxt;

  @Override
  public IndexType getIndexType() {
    return IndexType.DICTIONARY;
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
  public void index(IndexContext context) {
    try {
      URL data = this.getClass().getResource(wordDictionaryTxt);
      if (data == null) {
        throw new IOException(wordDictionaryTxt + " not found.");
      }
      File file = new File(data.getPath());
      int count = 0;
      try (IndexWriter writer = context.getWriter(); BufferedReader reader = new BufferedReader(new FileReader(file))) {
        logger.info("Indexing file {}, using writer {}", file, writer);
        writer.deleteAll(); //delete previously indexed data
        String line;
        while ((line = reader.readLine()) != null) {
          count++;
          String[] row = line.split("\\|");
          if (row.length < 4) {
            logger.error("Skipping malformed line : {}", line);
            continue;
          }
          // logger.info("Indexing dictionary row : {}", (Object) row);
          Document document = new Document();
          document.add(new TextField(IndexField.WORD.getName(), row[0], Field.Store.YES));
          document.add(new StringField(IndexField.PARTS_OF_SPEECH.getName(), row[1], Field.Store.YES));
          document.add(new TextField(IndexField.MEANING.getName(), row[2], Field.Store.YES));
          document.add(new StringField(IndexField.SOURCE.getName(), row[3], Field.Store.YES));
          writer.addDocument(document); //updateDocument if already exists?
        }
        writer.commit();
      }
      logger.info("Successfully Indexed {} words", count);
    } catch (IOException ioe) {
      logger.error(ioe.getMessage());
    }
  }

  @Override
  public List<DictionaryEntry> search(IndexContext context, SearchQuery searchQuery) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      String ques = searchQuery.getQuery();
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(IndexField.MEANING.getName(), context.getAnalyzer());
      Query query = parser.parse(ques);
      logger.info("Searching for the query : {}, using searcher : {}", query, searcher);

      TopDocs topDocs = searcher.search(query, 10);
      List<DictionaryEntry> result = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        logger.info("Explanation : {}", searcher.explain(query, scoreDoc.doc));
        Document document = searcher.storedFields().document(scoreDoc.doc);
        DictionaryEntry entry = DictionaryEntry.builder()
          .word(document.get(IndexField.WORD.getName()))
          .meaning(document.get(IndexField.MEANING.getName()))
          .partsOfSpeech(document.get(IndexField.PARTS_OF_SPEECH.getName()))
          .source(document.get(IndexField.SOURCE.getName()))
          .build();
        result.add(entry);
      }
      logger.info("Searched {} words for the question {}.", result.size(), ques);
      return result;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

}

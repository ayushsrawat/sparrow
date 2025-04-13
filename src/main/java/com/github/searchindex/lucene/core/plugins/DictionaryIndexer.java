package com.github.searchindex.lucene.core.plugins;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.Indexer;
import com.github.searchindex.lucene.core.entry.DictionaryEntry;
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
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Service
public class DictionaryIndexer implements Indexer<DictionaryEntry> {

  private static final Logger logger = LoggerFactory.getLogger(DictionaryIndexer.class);
  private static final String WORD_DICTIONARY_TXT = "/data/word-dictionary.txt";

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
      URL data = this.getClass().getResource(WORD_DICTIONARY_TXT);
      if (data == null) {
        throw new IOException(WORD_DICTIONARY_TXT + " not found.");
      }
      File file = new File(data.getPath());
      try (IndexWriter writer = context.getWriter(); BufferedReader reader = new BufferedReader(new FileReader(file))) {
        logger.info("Indexing file {}, using writer {}", file, writer);
        writer.deleteAll(); //delete previously indexed data
        String line;
        while ((line = reader.readLine()) != null) {
          String[] row = line.split("\\|");
          if (row.length < 4) {
            logger.error("Skipping malformed line : {}", line);
            continue;
          }
          logger.info("Indexing row : {}", (Object) row);
          Document document = new Document();
          document.add(new TextField(IndexField.WORD.getName(), row[0], Field.Store.YES));
          document.add(new StringField(IndexField.PARTS_OF_SPEECH.getName(), row[1], Field.Store.YES));
          document.add(new TextField(IndexField.MEANING.getName(), row[2], Field.Store.YES));
          document.add(new StringField(IndexField.SOURCE.getName(), row[3], Field.Store.YES));
          writer.addDocument(document); //updateDocument if already exists?
        }
        writer.commit();
      }
    } catch (IOException ioe) {
      logger.error(ioe.getMessage());
    }
  }

  @Override
  public List<DictionaryEntry> search(IndexContext context, String ques) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(IndexField.MEANING.getName(), context.getAnalyzer());
      Query query = parser.parse(ques);
      logger.info("Searching for the query : {}, using searcher : {}", ques, searcher);

      TopDocs topDocs = searcher.search(query, 10);
      List<DictionaryEntry> result = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        Document document = searcher.storedFields().document(scoreDoc.doc);
        DictionaryEntry entry = DictionaryEntry.builder()
          .word(document.get(IndexField.WORD.getName()))
          .meaning(document.get(IndexField.MEANING.getName()))
          .partsOfSpeech(document.get(IndexField.PARTS_OF_SPEECH.getName()))
          .source(document.get(IndexField.SOURCE.getName()))
          .build();
        result.add(entry);
      }
      return result;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

}

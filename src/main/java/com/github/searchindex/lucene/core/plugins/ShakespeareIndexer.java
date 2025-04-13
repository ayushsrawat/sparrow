package com.github.searchindex.lucene.core.plugins;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.Indexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//@Service
//work is in progress
public class ShakespeareIndexer implements Indexer<String> {

  private static final Logger logger = LoggerFactory.getLogger(ShakespeareIndexer.class);
  private static final String SHAKESPEARE_TXT = "/data/t8.shakespeare.txt";
  private static final Pattern PLAY_HEADER = Pattern.compile("^([A-Z][A-Z ,']+)$");

  @Override
  public IndexType getIndexType() {
    return IndexType.SHAKESPEARE;
  }

  @Override
  public void index(IndexContext context) {
    try {
      URL data = this.getClass().getResource(SHAKESPEARE_TXT);
      if (data == null) {
        throw new IOException(SHAKESPEARE_TXT + " not found.");
      }
      File file = Paths.get(data.toURI()).toFile();
      IndexWriter writer = context.getWriter();
      String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);

      Matcher play = PLAY_HEADER.matcher(content);
      Map<Integer, String> playMap = new TreeMap<>();
      while (play.find()) {
        String header = play.group(1).trim();
        logger.info("Play Header : {}", header);
        playMap.put(play.start(), header);
      }
      logger.info("Indexing complete : ");

      Document document = new Document();
      document.add(new TextField("name", file.getName(), Field.Store.YES));
      document.add(new TextField("content", content, Field.Store.YES));
      writer.addDocument(document);
      writer.commit();
      writer.close();
    } catch (URISyntaxException e) {
      logger.error(e.getMessage());
    } catch (IOException ioe) {
      logger.error(ioe.getMessage());
      throw new RuntimeException();
    }
  }

  @Override
  public List<String> search(IndexContext context, String query) {
    return List.of();
  }

}

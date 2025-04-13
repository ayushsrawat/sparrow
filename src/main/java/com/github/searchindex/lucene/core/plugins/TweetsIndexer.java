package com.github.searchindex.lucene.core.plugins;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.core.IndexType;
import com.github.searchindex.lucene.core.Indexer;
import com.github.searchindex.lucene.core.entry.SearchQuery;
import com.github.searchindex.lucene.core.entry.Tweet;
import com.github.searchindex.lucene.core.tools.DateUtil;
import com.github.searchindex.lucene.core.tools.ParseUtil;
import com.github.searchindex.lucene.core.tools.TweetNormalizer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
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

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@PropertySource("classpath:index.properties")
public class TweetsIndexer implements Indexer<Tweet> {

  private static final Logger logger = LoggerFactory.getLogger(TweetsIndexer.class);
  private static final String DATASET_V1_FILENAME = "twitter-dataset-v1.json";
  private static final String DATASET_V2_FILENAME = "twitter-dataset-v2.json";
  private static final String DATASET_V3_FILENAME = "twitter-dataset-v3.json";

  private final TweetNormalizer tweetNormalizer;
  private final ObjectMapper objectMapper;
  private final DateUtil dateUtil;
  private final ParseUtil parseUtil;

  @Value("${tweet.output.json.path}")
  private String twitterDatasetDirectory;

  @Override
  public IndexType getIndexType() {
    return IndexType.TWEETS;
  }

  @Getter
  private enum IndexField {
    TWEET_ID("tweet-id"),
    USERNAME("username"),
    FULL_NAME("full-name"),
    TWEET("tweet"),
    URL("url"),
    VIEWS("views"),
    LIKES("likes"),
    RETWEETS("retweets"),
    DATE("date"),
    FORMATTED_DATE("formatted_date");
    private final String name;

    IndexField(String name) {
      this.name = name;
    }
  }

  @Override
  public void index(IndexContext context) {
    try {
      tweetNormalizer.normalizeCsv();
      context.getWriter().deleteAll();
      int sum = 0;
      sum += indexTwitterDataset(context, DATASET_V1_FILENAME);
      sum += indexTwitterDataset(context, DATASET_V2_FILENAME);
      sum += indexTwitterDataset(context, DATASET_V3_FILENAME);
      context.getWriter().close();
      logger.info("Successfully Indexed {} tweets", sum);
    } catch (IOException ioe) {
      logger.error("Error indexing Tweets  : {}", ioe.getMessage());
    }
  }

  private int indexTwitterDataset(IndexContext context, String datasetFileName) {
    File twitterDataset = new File(twitterDatasetDirectory + "/tweets/" + datasetFileName);
    if (!twitterDataset.exists()) {
      logger.error("{} not found.", twitterDataset.getAbsolutePath());
      return 0;
    }
    try {
      List<Tweet> tweets = objectMapper
        .readValue(twitterDataset, new TypeReference<>() {});
      logger.info("Loaded {} tweets from {}", tweets.size(), twitterDataset.getName());
      for (Tweet tweet : tweets) {
        // this loop runs for 100_000+ times; can consider a better approach?
        logger.info("Indexing tweet >> {} : {} ", tweet.getUsername(), tweet.getTweet());
        Document document = new Document();
        document.add(new LongField(IndexField.TWEET_ID.getName(), tweet.getTweetId(), Field.Store.YES));
        document.add(new StringField(IndexField.USERNAME.getName(), tweet.getUsername(), Field.Store.YES));
        document.add(new TextField(IndexField.TWEET.getName(), tweet.getTweet(), Field.Store.YES));
        if (tweet.getTweetDate() != null) {
          long dateLong = dateUtil.convertToLong(tweet.getTweetDate());
          DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
          document.add(new LongPoint(IndexField.DATE.getName(), dateLong));
          document.add(new StoredField(IndexField.DATE.getName(), dateLong));
          document.add(new StringField(IndexField.FORMATTED_DATE.getName(), tweet.getTweetDate().format(formatter), Field.Store.YES));
        }
        if (tweet.getFullName() != null) {
          document.add(new TextField(IndexField.FULL_NAME.getName(), tweet.getFullName(), Field.Store.YES));
        }
        if (tweet.getUrl() != null) {
          document.add(new TextField(IndexField.URL.getName(), tweet.getUrl(), Field.Store.YES));
        }
        if (tweet.getViews() != null) {
          document.add(new IntField(IndexField.VIEWS.getName(), tweet.getViews(), Field.Store.YES));
        }
        if (tweet.getLikes() != null) {
          document.add(new IntField(IndexField.LIKES.getName(), tweet.getLikes(), Field.Store.YES));
        }
        if (tweet.getRetweets() != null) {
          document.add(new IntField(IndexField.RETWEETS.getName(), tweet.getRetweets(), Field.Store.YES));
        }
        context.getWriter().addDocument(document);
      }
      context.getWriter().commit();
      return tweets.size();
    } catch (IOException ioe) {
      logger.error("Failed to read tweets from {}: {}", twitterDataset.getName(), ioe.getMessage());
      return 0;
    }
  }

  @Override
  public List<Tweet> search(IndexContext context, SearchQuery searchQuery) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(IndexField.TWEET.getName(), context.getAnalyzer());
      Query query = parser.parse(searchQuery.getQuery());
      logger.info("Searching for the query : {}, using searcher : {}", query, searcher);

      TopDocs topDocs = searcher.search(query, 10);
      List<Tweet> result = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        Document document = searcher.storedFields().document(scoreDoc.doc);
        Tweet tweet = Tweet.builder()
          .tweetId(parseUtil.parseLong(document.get(IndexField.TWEET_ID.getName())))
          .username(document.get(IndexField.USERNAME.getName()))
          .tweet(document.get(IndexField.TWEET.getName()))
          .tweetDate(dateUtil.convertToLocalDateTime(
            parseUtil.parseLong(document.get(IndexField.DATE.getName()))))
          .fullName(document.get(IndexField.FULL_NAME.getName()))
          .url(document.get(IndexField.URL.getName()))
          .views(parseUtil.parseInt(document.get(IndexField.VIEWS.getName())))
          .likes(parseUtil.parseInt(document.get(IndexField.LIKES.getName())))
          .retweets(parseUtil.parseInt(document.get(IndexField.RETWEETS.getName())))
          .build();
        result.add(tweet);
      }
      logger.info("Searched {} tweets for the query {}.", result.size(), searchQuery.getQuery());
      return result;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

}
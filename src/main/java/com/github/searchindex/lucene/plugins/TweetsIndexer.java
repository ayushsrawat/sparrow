package com.github.searchindex.lucene.plugins;

import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexType;
import com.github.searchindex.lucene.Indexer;
import com.github.searchindex.lucene.TweetNormalizer;
import com.github.searchindex.lucene.entry.SearchQuery;
import com.github.searchindex.lucene.entry.Tweet;
import com.github.searchindex.lucene.tools.DateUtil;
import com.github.searchindex.lucene.tools.ParseUtil;
import lombok.Getter;
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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Service
@PropertySource("classpath:index.properties")
public class TweetsIndexer implements Indexer<Tweet> {

  private static final Logger logger = LoggerFactory.getLogger(TweetsIndexer.class);

  private final TweetNormalizer tweetNormalizer;
  private final DateUtil dateUtil;
  private final ParseUtil parseUtil;

  public TweetsIndexer(
    @Value("${normalizer.mode.db}") boolean useDbNormalizer,
    @Qualifier("tweetDbNormalizer") TweetNormalizer dbNormalizer,
    @Qualifier("tweetJsonNormalizer") TweetNormalizer jsonNormalizer,
    DateUtil dateUtil, ParseUtil parseUtil
  ) {
    this.tweetNormalizer = useDbNormalizer ? dbNormalizer : jsonNormalizer;
    this.dateUtil = dateUtil;
    this.parseUtil = parseUtil;
  }

  @Value("${twitter.index.batch.commit.size}")
  private Integer maxBatchCommitSize;

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
      int sum = indexTwitterDataset(context, tweetNormalizer.getNormalizedTweets());
      context.getWriter().close();
      logger.info("Successfully Indexed {} tweets", sum);
    } catch (IOException ioe) {
      logger.error("Error indexing Tweets  : {}", ioe.getMessage());
    }
  }

  private int indexTwitterDataset(IndexContext context, List<Tweet> tweets) {
    try {
      int batch = 0;
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
        if (++batch > maxBatchCommitSize) {
          context.getWriter().commit();
          batch = 0;
        }
      }
      context.getWriter().commit();
      return tweets.size();
    } catch (IOException ioe) {
      logger.error("Failed to index {} tweets", tweets.size(), ioe);
      return -1;
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
        result.add(extractTweetFromDocument(document));
      }
      logger.info("Searched {} tweets for the query {}.", result.size(), searchQuery.getQuery());
      return result;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

  private Tweet extractTweetFromDocument(Document document) {
    return Tweet.builder()
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
  }

  public List<Tweet> getIndexedTweets(IndexContext context) {
    List<Tweet> indexedTweets = new ArrayList<>();
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      for (LeafReaderContext leafContext : reader.leaves()) {
        LeafReader leafReader = leafContext.reader();
        Bits liveDocs = leafReader.getLiveDocs();
        for (int i = 0; i < leafReader.maxDoc(); i++) {
          if (liveDocs == null || liveDocs.get(i)) {
            Document document = leafReader.storedFields().document(i);
            indexedTweets.add(extractTweetFromDocument(document));
          }
        }
      }
      logger.info("Total Indexed Tweets (leaf reader): {}", indexedTweets.size());
    } catch (IOException ioe) {
      logger.error("Error reading index: {}", ioe.getMessage(), ioe);
    }
    return indexedTweets;
  }

  public List<Tweet> searchByUsername(IndexContext context, String username) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      IndexSearcher searcher = new IndexSearcher(reader);
      Query query = new TermQuery(new Term(IndexField.USERNAME.getName(), username));
      logger.info("Tweets hit count {} by username {}", searcher.count(query), username);
      return searcher.search(query, new CollectorManager<TweetsCollector, List<Tweet>>() {
        @Override
        public TweetsCollector newCollector() {
          return new TweetsCollector();
        }

        @Override
        public List<Tweet> reduce(Collection<TweetsCollector> collectors) {
          List<Tweet> tweetsByUser = new ArrayList<>();
          for (TweetsCollector collector : collectors) {
            tweetsByUser.addAll(collector.getResults());
          }
          return tweetsByUser;
        }
      });

    } catch (IOException ioe) {
      logger.error("Error reading index for user {}: {}", username, ioe.getMessage(), ioe);
      return List.of();
    }
  }

  private final class TweetsCollector extends SimpleCollector {
    private LeafReaderContext context;
    private Scorable scorer;

    @Getter
    private final List<Tweet> results = new ArrayList<>();

    @Override
    public void doSetNextReader(LeafReaderContext context) {
      this.context = context;
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int docId) throws IOException {
      Document doc = context.reader().storedFields().document(docId);
      results.add(extractTweetFromDocument(doc));
      logger.info("Score for docId {}: {}", docId, scorer.score());
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

}
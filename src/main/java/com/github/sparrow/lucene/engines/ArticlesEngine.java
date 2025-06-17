package com.github.sparrow.lucene.engines;

import com.github.sparrow.dto.ArticleSearchResponse;
import com.github.sparrow.entity.CrawledPage;
import com.github.sparrow.exception.IndexingException;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.Indexer;
import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.Searcher;
import com.github.sparrow.lucene.entity.SearchHit;
import com.github.sparrow.lucene.entity.SearchQuery;
import com.github.sparrow.util.DateUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

@Service
@RequiredArgsConstructor
public class ArticlesEngine implements Indexer<CrawledPage>, Searcher<SearchHit<ArticleSearchResponse>> {

  private static final Logger logger = LoggerFactory.getLogger(ArticlesEngine.class);
  private static final Integer MAX_FRAGMENT_SIZE = 150;

  private final DateUtil dateUtil;

  @Override
  public EngineType getEngineType() {
    return EngineType.ARTICLES;
  }

  @Getter
  public enum IndexField {
    ID("id"),
    URL("url"),
    TITLE("title"),
    CONTENT("content"),
    CRAWLED_AT("crawled-at"),
    CONTENT_HASH("content-hash");
    private final String name;

    IndexField(String name) {
      this.name = name;
    }
  }

  @Override
  public void index(LuceneContext context) throws IndexingException {
    /// articles are crawled by spiders one by one >> no operation
  }

  @Override
  public void indexDocument(LuceneContext context, CrawledPage article) throws IOException {
    logger.info("Indexing article: {}", article.getUrl());
    Document document = new Document();
    document.add(new IntField(IndexField.ID.getName(), article.getId(), Field.Store.YES));
    document.add(new StringField(IndexField.URL.getName(), article.getUrl(), Field.Store.YES));
    document.add(new StringField(IndexField.TITLE.getName(), article.getTitle(), Field.Store.YES));
    // store content for highlighting
    document.add(new TextField(IndexField.CONTENT.getName(), article.getContent(), Field.Store.YES));
    document.add(new LongPoint(IndexField.CRAWLED_AT.getName(), dateUtil.convertToLong(article.getLastCrawledAt())));
    document.add(new StringField(IndexField.CONTENT_HASH.getName(), article.getContentHash(), Field.Store.YES));
    context.getWriter().updateDocument(new Term(IndexField.URL.getName(), article.getUrl()), document);
    context.getWriter().commit();
  }

  @Override
  public List<SearchHit<ArticleSearchResponse>> search(LuceneContext context, SearchQuery searchQuery) {
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser queryParser = new QueryParser(IndexField.CONTENT.getName(), context.getAnalyzer());
      Query query = queryParser.parse(searchQuery.getQuery());

      Formatter formatter = new SimpleHTMLFormatter();
      QueryScorer scorer = new QueryScorer(query);
      Highlighter highlighter = new Highlighter(formatter, scorer);
      Fragmenter fragmenter = new SimpleSpanFragmenter(scorer, MAX_FRAGMENT_SIZE);
      highlighter.setTextFragmenter(fragmenter);
      var hits =  searcher.search(query,
        new CollectorManager<ArticlesCollector, PriorityQueue<SearchHit<ArticleSearchResponse>>>() {
        @Override
        public ArticlesCollector newCollector() {
          return new ArticlesCollector();
        }

        @Override
        public PriorityQueue<SearchHit<ArticleSearchResponse>> reduce(Collection<ArticlesCollector> collectors)
          throws IOException {
          final PriorityQueue<SearchHit<ArticleSearchResponse>> hits = new PriorityQueue<>(SearchHit::compareTo);
          for (ArticlesCollector collector : collectors) {
            for (SearchHit<ArticleSearchResponse> hit : collector.getHits()) {
              ArticleSearchResponse searchedArticle = hit.getHit();
              String content = searchedArticle.getContent();
              TokenStream tokenStream =
                context.getAnalyzer().tokenStream(IndexField.CONTENT.getName(), new StringReader(content));
              String highlighted;
              try {
                highlighted = highlighter.getBestFragment(tokenStream, content);
              } catch (InvalidTokenOffsetsException e) {
                highlighted = null;
              }
              if (highlighted == null) {
                highlighted = content.length() > MAX_FRAGMENT_SIZE
                  ? content.substring(0, MAX_FRAGMENT_SIZE) + "..."
                  : content;
              }
              searchedArticle.setContent(highlighted);
              hits.offer(hit);
            }
          }
          return hits;
        }
      });
      logger.info("Searched [{}] pages for query [{}]", hits.size(), query);
      List<SearchHit<ArticleSearchResponse>> results = new ArrayList<>();
      final int n = hits.size();
      searchQuery.setTopN(searchQuery.getTopN() == null ? Integer.MAX_VALUE : searchQuery.getTopN());
      while (!hits.isEmpty() && n - hits.size() < searchQuery.getTopN()) {
        results.add(hits.poll());
      }
      return results;
    } catch (IOException | ParseException e) {
      logger.error("Search failed : {}", e.getMessage());
      return List.of();
    }
  }

  public List<String> getIndexedTokens(LuceneContext context, IndexField indexField) {
    List<String> tokens = new ArrayList<>();
    try (IndexReader reader = DirectoryReader.open(context.getDirectory())) {
      for (LeafReaderContext leafContext : reader.leaves()) {
        Terms terms = leafContext.reader().terms(indexField.getName());
        if (terms == null) continue;
        TermsEnum iterator = terms.iterator();
        BytesRef term;
        while ((term = iterator.next()) != null) {
          tokens.add(term.utf8ToString());
        }
      }
    } catch (IOException ioe) {
      logger.error("Error reading index: {}", ioe.getMessage(), ioe);
    }
    return tokens;
  }

  @Getter
  private static class ArticlesCollector implements Collector {
    // todo: consider using TopDocsCollector for pagination of articles

    private final List<SearchHit<ArticleSearchResponse>> hits = new ArrayList<>();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new LeafCollector() {
        final int docBase = context.docBase;
        private Scorable scorer;

        @Override
        public void setScorer(Scorable scorer) {
          this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
          Document document = context.reader().storedFields().document(doc);
          ArticleSearchResponse article = ArticleSearchResponse
            .builder()
            .url(document.get(IndexField.URL.getName()))
            .title(document.get(IndexField.TITLE.getName()))
            .content(document.get(IndexField.CONTENT.getName()))
            .contentHash(document.get(IndexField.CONTENT_HASH.getName()))
            .build();
          hits.add(new SearchHit<>(article, scorer.score(), docBase + doc));
        }
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.TOP_DOCS_WITH_SCORES;
    }
  }

}

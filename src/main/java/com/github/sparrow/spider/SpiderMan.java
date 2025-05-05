package com.github.sparrow.spider;

import com.github.sparrow.entity.Article;
import com.github.sparrow.entity.CrawledPage;
import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.engines.ArticlesEngine;
import com.github.sparrow.repository.ArticleRepository;
import com.github.sparrow.repository.CrawledPageRepository;
import com.github.sparrow.util.HashUtil;
import lombok.RequiredArgsConstructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class SpiderMan {

  private static final Logger logger = LoggerFactory.getLogger(SpiderMan.class);

  private final ArticlesEngine articlesEngine;
  private final ArticleRepository articleRepository;
  private final CrawledPageRepository crawledPageRepository;
  private final LuceneContextFactory contextFactory;
  private final HashUtil hashUtil;

  @Value("${spider.retries.max}")
  private Integer maxRetries;
  @Value("${spider.depth.max}")
  private Integer maxDepth;

  public void activatePowers(JobExecutionContext jobContext) throws IOException {
    logger.info("SpiderMan Scheduling at: {}", jobContext.getScheduledFireTime());
    List<Article> articles = articleRepository.getSchedulingArticles(SpiderStatus.PENDING, SpiderStatus.FAILED, maxRetries);
    if (articles.isEmpty()) {
      logger.info("No articles to crawl. Scheduling next at {}", jobContext.getNextFireTime());
      return;
    }
    try (LuceneContext luceneContext = contextFactory.createLuceneContext(EngineType.ARTICLES, LuceneMode.INDEXING)) {
      for (Article article : articles) {
        crawlArticle(luceneContext, article);
      }
    }
  }

  private void crawlArticle(LuceneContext context, Article article) {
    logger.info("crawling Url: {}", article.getUrl());
    article.setStatus(SpiderStatus.IN_PROGRESS);
    articleRepository.save(article);
    try {
      crawlUrlRecursively(context, article, article.getUrl(), 0, new HashSet<>());
      article.setStatus(SpiderStatus.CRAWLED);
    } catch (IOException ioe) {
      logger.error("Error crawling the article: {} ", article.getUrl(), ioe);
      article.setStatus(SpiderStatus.FAILED);
    }
    articleRepository.save(article);
  }

  private void crawlUrlRecursively(LuceneContext context, Article parent, String url, Integer depth, Set<String> visitedUrl) throws IOException {
    if (depth > maxDepth || visitedUrl.contains(url)) return;
    Optional<CrawledPage> isAlreadyCrawled = crawledPageRepository.getByUrl(url);
    if (isAlreadyCrawled.isPresent()) return;

    Document dom = Jsoup.connect(url).get();
    visitedUrl.add(url);
    String title = dom.title();
    logger.info("Crawled page title: {}", title);

    String content = dom.body().text();
    String contentHash = hashUtil.hashSHA256(content);
    CrawledPage crawledPage = CrawledPage
      .builder()
      .url(url)
      .parentArticle(parent)
      .title(title)
      .contentHash(contentHash)
      .content(content)
      .status(SpiderStatus.CRAWLED)
      .lastCrawledAt(LocalDateTime.now())
      .build();
    crawledPageRepository.save(crawledPage);
    // todo: in a separate thread >> lifecycle of executor service?
    articlesEngine.indexDocument(context, crawledPage);

    Elements links = dom.select("a[href]");
    for (Element link : links) {
      String href = link.absUrl("href");
      if (!href.isEmpty() && isSameDomain(url, href)) {
        try {
          crawlUrlRecursively(context, parent, href, depth + 1, visitedUrl);
        } catch (IOException ioe) {
          logger.warn("Failed to crawl link: {}", href, ioe);
        }
      }
    }
  }

  private boolean isSameDomain(String base, String link) {
    try {
      URI baseUri = new URI(base);
      URI linkUri = new URI(link);
      return baseUri.getHost() != null && baseUri.getHost().equals(linkUri.getHost());
    } catch (URISyntaxException e) {
      return false;
    }
  }

}

package com.github.searchindex.spider;

import com.github.searchindex.entity.Article;
import com.github.searchindex.entity.CrawledPage;
import com.github.searchindex.lucene.IndexContext;
import com.github.searchindex.lucene.IndexContextFactory;
import com.github.searchindex.lucene.IndexMode;
import com.github.searchindex.lucene.IndexType;
import com.github.searchindex.lucene.plugins.ArticlesIndexer;
import com.github.searchindex.repository.ArticleRepository;
import com.github.searchindex.repository.CrawledPageRepository;
import com.github.searchindex.util.HashUtil;
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

  private final ArticlesIndexer articlesIndexer;
  private final ArticleRepository articleRepository;
  private final CrawledPageRepository crawledPageRepository;
  private final IndexContextFactory contextFactory;
  private final HashUtil hashUtil;

  @Value("${spider.retries.max}")
  private Integer maxRetries;
  @Value("${spider.depth.max}")
  private Integer maxDepth;

  public void activatePowers(JobExecutionContext jobExecutionContext) throws IOException {
    logger.info("SpiderMan Scheduling at: {}", jobExecutionContext.getScheduledFireTime());
    IndexContext indexContext =  contextFactory.createIndexContext(IndexType.ARTICLES, IndexMode.INDEXING);
    List<Article> articles = articleRepository.getSchedulingArticles(SpiderStatus.PENDING, SpiderStatus.FAILED, maxRetries);
    for (Article article : articles) {
      crawlArticle(indexContext, article);
    }
    indexContext.getWriter().close();
    indexContext.getDirectory().close();
  }

  private void crawlArticle(IndexContext context, Article article) {
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

  private void crawlUrlRecursively(IndexContext context, Article parent, String url, Integer depth, Set<String> visitedUrl) throws IOException {
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
    articlesIndexer.indexDocument(context, crawledPage);

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

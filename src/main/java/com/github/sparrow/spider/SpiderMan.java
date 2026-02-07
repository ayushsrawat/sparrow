package com.github.sparrow.spider;

import com.github.sparrow.entity.Article;
import com.github.sparrow.entity.CrawledPage;
import com.github.sparrow.lucene.EngineType;
import com.github.sparrow.lucene.LuceneContext;
import com.github.sparrow.lucene.LuceneContextFactory;
import com.github.sparrow.lucene.LuceneMode;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class SpiderMan {

  private static final Logger logger = LoggerFactory.getLogger(SpiderMan.class);
  private static final Pattern urlPattern = Pattern.compile("https?://(\\S*)");

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
    if (articles.isEmpty()) { // todo what is maxRetries above if you are not updating it?
      logger.info("No articles to crawl. Scheduling next at {}", jobContext.getNextFireTime());
      return;
    }
    try (LuceneContext luceneContext = contextFactory.createLuceneContext(EngineType.ARTICLES, LuceneMode.INDEXING)) {
      for (Article article : articles) {
        article.setStatus(SpiderStatus.IN_PROGRESS);
        articleRepository.save(article);
      }
      for (Article article : articles) {
        crawlArticle(luceneContext, article);
      }
    }
  }

  private void crawlArticle(LuceneContext context, Article article) {
    logger.info("crawling Url: {}", article.getUrl());
    try {
      crawlUrlRecursively(context, article, article.getUrl(), 0, new HashSet<>());
      article.setStatus(SpiderStatus.CRAWLED);
    } catch (IOException ioe) {
      logger.error("Error crawling the article: {} ", article.getUrl(), ioe);
      article.setStatus(SpiderStatus.FAILED);
    }
    article.setLastCrawledAt(LocalDateTime.now());
    articleRepository.save(article);
  }

  private void crawlUrlRecursively(LuceneContext context, Article parent, String url, Integer depth, Set<String> visitedUrl) throws IOException {
    if (depth > maxDepth || visitedUrl.contains(url)) return;
    Optional<CrawledPage> isAlreadyCrawled = crawledPageRepository.getByUrl(url);
    if (isAlreadyCrawled.isPresent()) return;

    Scrapper scrapper = new Scrapper(url, new Scrapper.Callback() {
      @Override
      public void success(String content, String contentType) {
        String title = url;
        String body = content;
        List<String> childUrls = new ArrayList<>();
        if (contentType.contains("html")) {
          Document dom = Jsoup.parse(content);
          visitedUrl.add(url);
          title = dom.title();
          logger.info("Crawled page title: {}", title);
          body = dom.body().text();
          Elements links = dom.select("a[href]");
          for (Element link : links) {
            childUrls.add(link.absUrl("href"));
          }
        } else if (contentType.contains("plain")) {
          Matcher matcher = urlPattern.matcher(content);
          String[] urls = matcher.results().map(MatchResult::group).toArray(String[]::new);
          logger.info("Found {} urls from parent url [{}]", urls.length, url);
          childUrls.addAll(Arrays.asList(urls));
        } else {
          logger.warn("Unsupported content type for url [{}]", url);
          return;
        }
        String contentHash = hashUtil.hashSHA256(body);
        CrawledPage crawledPage = CrawledPage
          .builder()
          .url(url)
          .parentArticle(parent)
          .title(title)
          .contentHash(contentHash)
          .content(body)
          .contentType(contentType)
          .status(SpiderStatus.CRAWLED)
          .lastCrawledAt(LocalDateTime.now())
          .build();
        crawledPageRepository.save(crawledPage);
        try {
          articlesEngine.indexDocument(context, crawledPage);
        } catch (IOException ioe) {
          logger.error("Unable to index Scrapped document {} : {}", url, ioe.getMessage());
        }
        for (String childUrl : childUrls) {
          if (!childUrl.isEmpty() /*&& isSameDomain(url, childUrl)*/) {
            try {
              crawlUrlRecursively(context, parent, childUrl, depth + 1, visitedUrl);
            } catch (IOException ioe) {
              logger.warn("Failed to crawl link: {}", childUrl, ioe);
            }
          }
        }
      }

      @Override
      public void failure(Throwable t) {
        logger.error(t.getMessage(), t);
      }
    });
    scrapper.run();
  }

  @SuppressWarnings("unused")
  private boolean isSameDomain(String base, String link) {
    try {
      URI baseUri = new URI(base);
      URI linkUri = new URI(link);
      return baseUri.getHost() != null && baseUri.getHost().equals(linkUri.getHost());
    } catch (URISyntaxException e) {
      return false;
    }
  }

  @SuppressWarnings("unused")
  public String normalizeUrl(String url) {
    try {
      URI uri = new URI(url);
      return new URI(uri.getScheme(), uri.getHost(), uri.getPath(), null).toString();
    } catch (URISyntaxException e) {
      return url;
    }
  }
}

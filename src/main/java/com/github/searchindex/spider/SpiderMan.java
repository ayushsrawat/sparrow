package com.github.searchindex.spider;

import com.github.searchindex.entity.Article;
import com.github.searchindex.repository.ArticleRepository;
import com.github.searchindex.repository.CrawledPageRepository;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class SpiderMan {

  private static final Logger logger = LoggerFactory.getLogger(SpiderMan.class);

  private final ArticleRepository articleRepository;
  private final CrawledPageRepository crawledPageRepository;

  @Value("${spider.retries.max}")
  private Integer maxRetries;
  @Value("${spider.depth.max}")
  private Integer maxDepth;

  public void start(JobExecutionContext context) {
    logger.info("SpiderMan Scheduling at: {}", context.getScheduledFireTime());
    List<Article> articles = articleRepository.getSchedulingArticles(SpiderStatus.PENDING, SpiderStatus.FAILED, maxRetries);
    for (Article article : articles) {
      crawlArticle(article);
    }
  }

  private void crawlArticle(Article article) {
    logger.info("crawling Url: {}", article.getUrl());
    article.setStatus(SpiderStatus.IN_PROGRESS);
    articleRepository.save(article);
    try {
      crawlUrl(article, article.getUrl(), 0, new HashSet<>());
      article.setStatus(SpiderStatus.PENDING);
    } catch (IOException ioe) {
      logger.error("Error crawling the article: {} ", article.getUrl(), ioe);
      article.setStatus(SpiderStatus.FAILED);
    }
    articleRepository.save(article);
  }

  private void crawlUrl(Article parent, String url, Integer depth, Set<String> visitedUrl) throws IOException {
    if (depth > maxDepth || visitedUrl.contains(url)) return;

    Document dom = Jsoup.connect(url).get();
    visitedUrl.add(url);
    logger.info("Title: {}", dom.title());

    // todo: index this content with stop common words
    //       save this visited url to CrawledPage
    String content = dom.body().text();

    Elements links = dom.select("a[href]");
    for (Element link : links) {
      String href = link.absUrl("href");
      if (!href.isEmpty() && !visitedUrl.contains(href) && isSameDomain(url, href)) {
        try {
          crawlUrl(parent, href, depth + 1, visitedUrl);
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

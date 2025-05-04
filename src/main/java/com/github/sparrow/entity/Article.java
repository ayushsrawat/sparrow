package com.github.sparrow.entity;

import com.github.sparrow.spider.SpiderStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "articles")
@Getter
@Setter
public class Article {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Integer id;

  @Column(length = 50, nullable = false)
  private String title;

  @Column(unique = true, nullable = false)
  private String url;

  private String author;

  @Enumerated(EnumType.STRING)
  @Column(name = "crawl_status")
  private SpiderStatus status;

  private Integer priority;

  private Integer retries;

  @Column(name = "last_crawled_at")
  private LocalDateTime lastCrawledAt;

  @OneToMany(mappedBy = "parentArticle", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  private List<CrawledPage> crawledPages;

  @Override
  public String toString() {
    return "\n Author: " + this.author +
      "\n Url: " + this.url +
      // "\n Title: " + this.title +
      "\n Crawling Status: " + this.status;
  }

}

package com.github.searchindex.entity;

import com.github.searchindex.spider.SpiderStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "crawled_pages")
@Getter
public class CrawledPage {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Integer id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "parent_id")
  private Article parentArticle;

  @Column(nullable = false)
  private String url;

  private String title;

  @Enumerated(EnumType.STRING)
  @Column(name = "crawl_status")
  private SpiderStatus status;

  @Column(name = "last_crawled_at")
  private LocalDateTime lastCrawledAt;

  @Column(name = "content_hash")
  private String contentHash;

}

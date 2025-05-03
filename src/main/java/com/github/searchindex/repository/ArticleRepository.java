package com.github.searchindex.repository;

import com.github.searchindex.entity.Article;
import com.github.searchindex.spider.SpiderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ArticleRepository extends JpaRepository<Article, Integer> {

  @Query(value = "select a from Article a where a.status = :pending or (a.status = :failed and a.retries < :maxRetries)")
  List<Article> getSchedulingArticles(
    @Param("pending") SpiderStatus pending,
    @Param("failed") SpiderStatus failed,
    @Param("maxRetries") int maxRetries
  );


}

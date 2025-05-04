package com.github.searchindex.repository;

import com.github.searchindex.entity.CrawledPage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrawledPageRepository extends JpaRepository<CrawledPage, Integer> {

  Optional<CrawledPage> getByUrl(String url);

  @Query("select c from CrawledPage c where c.url in :urls")
  List<CrawledPage> findAllByUrls(@Param("urls") List<String> urls);

}

package com.github.searchindex.repository;

import com.github.searchindex.entity.CrawledPage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CrawledPageRepository extends JpaRepository<CrawledPage, Integer> {

  Optional<CrawledPage> getByUrl(String url);

}

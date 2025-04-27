package com.github.searchindex.repository;

import com.github.searchindex.entity.TweetData;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface TweetRepository extends JpaRepository<TweetData, Integer> {

  @Modifying
  @Transactional
  @Query("delete from TweetData")
  void deleteAllInBulk();

}

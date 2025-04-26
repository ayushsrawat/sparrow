package com.github.searchindex.repository;

import com.github.searchindex.entity.TweetData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TweetRepository extends JpaRepository<TweetData, Integer> {

}

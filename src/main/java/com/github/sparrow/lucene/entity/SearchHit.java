package com.github.sparrow.lucene.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SearchHit<T> implements Comparable<SearchHit<T>> {

  private T hit;
  private float score;
  private int docId;

  @Override
  public int compareTo(SearchHit<T> o) {
    if (this.getScore() > o.getScore()) return -1; // high score ^ priority
    if (this.getScore() < o.getScore()) return 1;
    return Integer.compare(this.getDocId(), o.getDocId());
  }

}

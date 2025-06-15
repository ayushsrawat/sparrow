package com.github.sparrow.lucene.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SearchHit<T> {

  private T hit;
  private float score;
  private int docId;

}

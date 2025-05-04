package com.github.searchindex.lucene;

import lombok.Getter;

@Getter
public enum IndexType {

  DICTIONARY("dictionary"),
  TWEETS("tweets"),
  ARTICLES("articles");

  private final String name;

  IndexType(String name) {
    this.name = name;
  }

}

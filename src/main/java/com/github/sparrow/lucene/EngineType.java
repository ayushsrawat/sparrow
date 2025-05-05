package com.github.sparrow.lucene;

import lombok.Getter;

@Getter
public enum EngineType {

  DICTIONARY("dictionary"),
  TWEETS("tweets"),
  ARTICLES("articles");

  private final String name;

  EngineType(String name) {
    this.name = name;
  }

}

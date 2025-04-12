package com.github.searchindex.lucene.core;

import lombok.Getter;

@Getter
public enum IndexType {

  DICTIONARY("dictionary"),
  SHAKESPEARE("shakespeare"),;

  private final String name;

  IndexType(String name) {
    this.name = name;
  }

}

package com.github.sparrow.lucene.entry;

import lombok.Builder;

@Builder
public record DictionaryEntry(
  String word,
  String meaning,
  String partsOfSpeech,
  String source
) {
  @Override
  public String toString() {
    return "Word: " + word +
      "\nMeaning: " + meaning +
      "\nPart of Speech: " + partsOfSpeech +
      "\nSource: " + source;
  }
}

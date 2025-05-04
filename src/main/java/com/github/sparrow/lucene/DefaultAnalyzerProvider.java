package com.github.sparrow.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.springframework.stereotype.Service;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.util.List;

@Service
public class DefaultAnalyzerProvider implements AnalyzerProvider {

  @Override
  public Analyzer getAnalyzer(IndexType indexType) {
    return switch (indexType) {
      case TWEETS -> new TweetsAnalyzer();
      case ARTICLES -> new ArticlesAnalyzer();
      default -> new StandardAnalyzer();
    };
  }

  private static class TweetsAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer source = new StandardTokenizer();
      TokenStream result = new EnglishPossessiveFilter(source);
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
      result = new SnowballFilter(result, new EnglishStemmer());
      return new TokenStreamComponents(source, result);
    }
  }

  private static class ArticlesAnalyzer extends Analyzer {

    private static final CharArraySet stopWords;

    static {
      stopWords = new CharArraySet(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, true);
      List<String> socialMediaWords = List.of(
        "facebook", "twitter", "x", "instagram", "snapchat", "tiktok",
        "youtube", "linkedin", "reddit", "threads", "telegram", "whatsapp",
        "follow", "like", "share", "subscribe", "dm", "post", "story", "hashtag", "reel"
      );
      stopWords.addAll(socialMediaWords);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer source = new StandardTokenizer();
      TokenStream result = new EnglishPossessiveFilter(source);
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, stopWords);
      result = new SnowballFilter(result, new EnglishStemmer());
      return new TokenStreamComponents(source, result);
    }
  }

}

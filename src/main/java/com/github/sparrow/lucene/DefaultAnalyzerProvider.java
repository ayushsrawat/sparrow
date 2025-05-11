package com.github.sparrow.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.springframework.stereotype.Service;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.util.List;

@Service
public class DefaultAnalyzerProvider implements AnalyzerProvider {

  @Override
  public Analyzer getAnalyzer(EngineType engineType, LuceneMode luceneMode, Boolean stemming) {
    return switch (engineType) {
      case TWEETS -> {
        if (LuceneMode.INDEXING.equals(luceneMode)) {
          yield new TweetsAnalyzer();
        }
        yield new TweetsSearchAnalyzer(stemming);
      }
      case ARTICLES -> {
        if (LuceneMode.INDEXING.equals(luceneMode)) {
          yield new ArticlesAnalyzer();
        }
        yield new ArticlesSearchAnalyzer(stemming);
      }
      default -> new StandardAnalyzer();
    };
  }

  /// tweets indexing analyzer
  private static class TweetsAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer source = new StandardTokenizer();
      TokenStream result = new EnglishPossessiveFilter(source);
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
      result = new KeywordRepeatFilter(result);
      result = new SnowballFilter(result, new EnglishStemmer());
      result = new RemoveDuplicatesTokenFilter(result);
      return new TokenStreamComponents(source, result);
    }
  }

  /// for searching stemming/un-stemming version of the tweets tokens
  private static class TweetsSearchAnalyzer extends Analyzer {

    private final boolean stemming;

    public TweetsSearchAnalyzer(Boolean stemming) {
      this.stemming = stemming;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer source = new StandardTokenizer();
      TokenStream result = new EnglishPossessiveFilter(source);
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
      if (stemming) {
        result = new SnowballFilter(result, new EnglishStemmer());
      }
      return new TokenStreamComponents(source, result);
    }
  }

  /// article indexing analyzer
  private static class ArticlesAnalyzer extends Analyzer {

    protected static final CharArraySet stopWords;

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
      result = new KeywordRepeatFilter(result);
      result = new SnowballFilter(result, new EnglishStemmer());
      result = new RemoveDuplicatesTokenFilter(result);
      return new TokenStreamComponents(source, result);
    }
  }

  /// for searching stemming/un-stemming version of the article tokens
  private static class ArticlesSearchAnalyzer extends Analyzer {

    private final boolean stemming;

    public ArticlesSearchAnalyzer(Boolean stemming) {
      this.stemming = stemming;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer source = new StandardTokenizer();
      TokenStream result = new EnglishPossessiveFilter(source);
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, ArticlesAnalyzer.stopWords);
      if (stemming) {
        result = new SnowballFilter(result, new EnglishStemmer());
      }
      return new TokenStreamComponents(source, result);
    }
  }

}

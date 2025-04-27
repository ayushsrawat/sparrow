package com.github.searchindex.lucene.entry;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents the search questions with different parameters <br>
 * anticipating this gonna get more complex with time
 */
@Getter
@Setter
@Builder
public class SearchQuery {

  private String query;
  private String username;

}

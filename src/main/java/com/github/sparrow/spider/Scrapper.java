package com.github.sparrow.spider;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Improvements:
 *  1) manage this scrapper via scrapperManager? like have a blocking query of 5 urls at a time and run them in async?
 *  2) take httpClient from the injection? no need to create new client every single request
 *  3) use rate limiter and politeness? from crawler-commons
 *  4) filter junk links
 * */
public class Scrapper implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(Scrapper.class);

  private final String url;
  private final Callback callback;

  public Scrapper(String url, Callback callback) {
    this.url = url;
    this.callback = callback;
  }

  @Override
  public void run() {
    OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(10, TimeUnit.SECONDS)
      .readTimeout(10, TimeUnit.SECONDS)
      .retryOnConnectionFailure(true)
      .build();

    Request request = new Request.Builder()
      .url(url)
      .header("User-Agent", "Mozilla/5.0 (compatible; SparrowBot)")
      .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful() || response.body() == null) {
        String msg = String.format("Error while crawling url: %s with status code: %d", url, response.code());
        throw new IOException(msg);
      }
      String contentType = response.header("content-type");
      String content = response.body().string();
      callback.success(content, contentType);
    } catch (IOException ioe) {
      logger.error(ioe.getMessage());
      callback.failure(ioe);
    }
  }

  public interface Callback {

    void success(String content, String contentType);

    void failure(Throwable t);
  }

}

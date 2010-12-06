package com.mycompany;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Sample application code.  For more examples visit http://community.jboss.org/wiki/5minutetutorialonInfinispan
 */
public class Application {

   public void basicUse() {
      // This cache stores arbitrary Strings
      Cache<String, String> cache = SampleCacheContainer.getCache();
      String oldValue = cache.put("Hello", "World");
      boolean worked = cache.replace("Hello", "World", "Mars");

      assert oldValue == null;
      assert worked == true;
   }

   public void lifespans() throws InterruptedException {
      // This cache stores stock prices with a limited lifespan
      Cache<String, Float> stocksCache = SampleCacheContainer.getCache("stock tickers");
      stocksCache.put("RHT", 45.0f, 10, TimeUnit.SECONDS);

      Thread.sleep(10000);

      assert stocksCache.get("RHT") == null;
   }

   public void asyncOperations() {
      // This cache stores wine bottle counts using the async API, allowing multiple writes to happen in parallel
      Cache<String, Integer> wineCache = SampleCacheContainer.getCache("wine cache");

      NotifyingFuture<Integer> f1 = wineCache.putAsync("Pinot Noir", 300);
      NotifyingFuture<Integer> f2 = wineCache.putAsync("Merlot", 120);
      NotifyingFuture<Integer> f3 = wineCache.putAsync("Chardonnay", 180);

      // now poll the futures to make sure any remote calls have completed!
      for (NotifyingFuture<Integer> f: Arrays.asList(f1, f2, f3)) {
         try {
            f.get();
         } catch (Exception e) {
            throw new RuntimeException("Operation failed!", e);
         }
      }

      // TIP: For more examples on using the asynchronous API, visit http://community.jboss.org/wiki/AsynchronousAPI
   }

   public void registeringListeners() {
      Cache<Integer, String> anotherCache = SampleCacheContainer.getCache("another");
      MyListener l = new MyListener();
      anotherCache.addListener(l);

      anotherCache.put(1, "One");
      anotherCache.put(2, "Two");
      anotherCache.put(3, "Three");

      // TIP: For more examples on using listeners visit http://community.jboss.org/wiki/ListenersandNotifications
   }
}

@Listener
class MyListener {

   @CacheEntryCreated
   @CacheEntryModified
   @CacheEntryRemoved
   public void printDetailsOnChange(CacheEntryEvent e) {
      System.out.printf("Thread %s has modified an entry in the cache named %s under key %s!",
                        Thread.currentThread().getName(), e.getCache().getName(), e.getKey());
   }

   @CacheEntryVisited
   public void pribtDetailsOnVisit(CacheEntryVisitedEvent e) {
      System.out.printf("Thread %s has visited an entry in the cache named %s under key %s!",
                        Thread.currentThread().getName(), e.getCache().getName(), e.getKey());
   }
}

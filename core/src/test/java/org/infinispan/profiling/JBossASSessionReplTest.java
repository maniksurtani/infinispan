/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.profiling;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This test mimics JBoss AS' usage of Infinispan for session replication, containing: - REPL_ASYNC - Batching - Atomic
 * Maps - Marshalled values
 *
 * @author Manik Surtani
 */
@Test(testName = "profiling.JBossASSessionReplTest", groups = "functional")
public class JBossASSessionReplTest extends MultipleCacheManagersTest {

   private static final AtomicInteger SESSION_GENERATOR = new AtomicInteger();
   private static final int WORKERS = 50;
   private static final int OPS_PER_TX = 5;
   private static final AtomicInteger SAMPLE_SIZE = new AtomicInteger(50000000);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb
            .clustering().cacheMode(CacheMode.REPL_ASYNC)
            .invocationBatching().enable()
            .storeAsBinary().enable().storeKeysAsBinary(false).storeValuesAsBinary(true);
      createCluster(cb, 2);
   }

   public void testSessionReplication() throws InterruptedException {
      cache(0).addListener(new Listener());
      cache(1).addListener(new Listener());

      List<Worker> workerList = new ArrayList<Worker>(WORKERS);
      final CountDownLatch startLatch = new CountDownLatch(1);
      for (int i = 0; i < WORKERS; i++) {
         Worker w = new Worker(cache(i % 2), startLatch);
         w.setDaemon(true);
         w.start();
         workerList.add(w);
      }

      long start = System.nanoTime();
      startLatch.countDown();

      for (Worker w : workerList) w.join();
      long endNanos = System.nanoTime() - start;

      System.out.printf("**** Test complete in %s ****%n", Util.prettyPrintTime(endNanos, TimeUnit.NANOSECONDS));
   }

   class Worker extends Thread {
      private final String sessionId;
      private final Cache<Object, Object> cache;
      private final CountDownLatch startLatch;

      Worker(Cache<Object, Object> cache, CountDownLatch startLatch) {
         int threadNumber = SESSION_GENERATOR.incrementAndGet();
         setName("Worker-" + threadNumber);
         sessionId = "SESSION-" + threadNumber;
         this.cache = cache;
         this.startLatch = startLatch;
      }

      @Override
      public void run() {
         try {
            startLatch.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         System.out.println("Processing session " + sessionId);
         int processed = 0;
         while (SAMPLE_SIZE.getAndDecrement() > 0) {
            if (processed % 1000 == 0) System.out.printf("%s processed %s sessions%n", getName(), processed);
            processSession();
            processed++;
         }
      }

      private void processSession() {
         try {
            // Start batch
            cache.getAdvancedCache().getTransactionManager().begin();
            // Create the session map
            Map<String, Value> s = AtomicMapLookup.getAtomicMap(cache, sessionId);

            // Lookup map again - as per AS access patterns
            s = AtomicMapLookup.getAtomicMap(cache, sessionId);
            for (int i = 0; i < OPS_PER_TX; i++) {
               s.put("attribute " + i, new Value("ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ_ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
            }
            cache.getAdvancedCache().getTransactionManager().commit();
         } catch (Exception e) {

         }
      }
   }

   @org.infinispan.notifications.Listener
   public static class Listener {
      @CacheEntryRemoved
      @CacheEntryActivated
      public void doNothing(CacheEntryEvent e) {

      }

      @CacheEntryModified
      public void doSomething(CacheEntryModifiedEvent e) {
         e.getValue();
      }
   }
}

class Value implements Serializable {
   private final String contents;

   Value(String contents) {
      this.contents = contents;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Value value = (Value) o;

      if (contents != null ? !contents.equals(value.contents) : value.contents != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return contents != null ? contents.hashCode() : 0;
   }
}
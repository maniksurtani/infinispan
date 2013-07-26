package org.infinispan.loaders.decorator;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.spi.BasicCacheLoader;
import org.infinispan.loaders.spi.BulkCacheLoader;
import org.infinispan.loaders.spi.ExpiryCacheLoader;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.infinispan.loaders.decorator.AbstractDelegatingCacheLoader.undelegateCacheLoader;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class ChainingCacheLoader implements BasicCacheLoader {

   private static final Log log = LogFactory.getLog(ChainingCacheLoader.class);

   private final ReadWriteLock loadersMutex = new ReentrantReadWriteLock();
   private final List<BasicCacheLoader> loaders = new LinkedList<BasicCacheLoader>();
   private BulkCacheLoader bulkCacheLoader;

   protected ExecutorService purgerService;

   private Cache cache;

   private CacheNotifier notifier;

   @Override
   public void init(CacheStoreConfig config, Cache cache, StreamingMarshaller m) {
      this.cache = cache;
   }

   @Override
   public void start() {
      purgerService = Executors.newFixedThreadPool(1, new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            String name = cache == null ? "" : cache.getName() + "- expiredEntryPurgingThread";
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
         }
      });
   }

   @Override
   public InternalCacheValue load(Object key) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void store(Object key, InternalCacheValue value) {
      // TODO: Customise this generated block
   }

   @Override
   public void stop() {
      purgerService.shutdownNow();
   }

   public void removeCacheLoader(String loaderType) {
      loadersMutex.writeLock().lock();
      try {
         Set<BasicCacheLoader> toRemove = new HashSet<BasicCacheLoader>();

         for (BasicCacheLoader cl : loaders) {
            String storeClass = undelegateCacheLoader(cl).getClass().getName();
            if (storeClass.equals(loaderType)) toRemove.add(cl);
         }

         for (BasicCacheLoader cl : toRemove) {
            try {
               log.debugf("Stopping and removing cache loader %s", loaderType);
               cl.stop();
            } catch (Exception e) {
               log.infof("Problems shutting down cache loader %s", loaderType, e);
            }
            loaders.remove(cl);
         }
      } finally {
         loadersMutex.writeLock().unlock();
      }
   }

   @Override
   public void clear() {
      // TODO: Customise this generated block
   }

   @Override
   public CacheStoreConfig getCacheLoaderConfig() {
      return null;  // TODO: Customise this generated block
   }

   public boolean isEmpty() {
      return loaders.isEmpty();
   }

   public void addCacheLoader(BasicCacheLoader loader, boolean isPreload) {
      loadersMutex.writeLock().lock();
      try {
         loaders.add(loader);
      } finally {
         loadersMutex.writeLock().unlock();
      }
      if (isPreload)
         if (! (loader instanceof BulkCacheLoader)) {
            throw new CacheException("The cache loader that is configured to preload must implement " +
                                           BulkCacheLoader.class.getName() + ". " + loader.getClass().getName() + " doesn't.");
         }
         bulkCacheLoader = (BulkCacheLoader) loader;
   }

   public void purgeIfNecessary() {
      loadersMutex.readLock().lock();
      try {
         for (BasicCacheLoader loader : loaders) {
            if (loader.getCacheLoaderConfig().isPurgeOnStartup())
               loader.clear();
         }
      } finally {
         loadersMutex.readLock().unlock();
      }
   }

   public void purgeExpired() {
      loadersMutex.readLock().lock();
      try {
         for (BasicCacheLoader loader : loaders) {
            if (loader instanceof ExpiryCacheLoader) {
               final ExpiryCacheLoader ecl = (ExpiryCacheLoader) loader;
               if (ecl.getCacheLoaderConfig().isPurgeSynchronously()) {
                   purgeAndNotify(ecl);
               } else {
                  purgerService.submit(new Runnable() {
                     @Override
                     public void run() {
                        purgeAndNotify(ecl);
                     }
                  });
               }
            }
         }
      } finally {
         loadersMutex.readLock().unlock();
      }
   }

   private void purgeAndNotify(ExpiryCacheLoader ecl) {
      //todo - do the expiration notification here: https://issues.jboss.org/browse/ISPN-3064
      Set purgedKeys = ecl.purgeExpired();
   }

   public <T extends BasicCacheLoader> List<T> getCacheLoaders(Class<T> loaderClass) {
      List<T> result = new ArrayList<T>();
      for (BasicCacheLoader bcl : loaders) {
         BasicCacheLoader cl = undelegateCacheLoader(bcl);
         if (loaderClass.isInstance(cl)) {
            result.add((T)cl);
         }
      }
      return result;
   }

   public List<BasicCacheLoader> getLoaders() {
      return loaders;
   }

   public BulkCacheLoader getBulkCacheLoader() {
      return bulkCacheLoader;
   }
}

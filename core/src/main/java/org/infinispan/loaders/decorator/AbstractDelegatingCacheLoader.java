package org.infinispan.loaders.decorator;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.spi.BasicCacheLoader;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class AbstractDelegatingCacheLoader {

   BasicCacheLoader delegate;

   public InternalCacheValue load(Object key) {
      return delegate.load(key);
   }

   public void start() {
      delegate.start();
   }

   public void stop() {
      delegate.stop();
   }

   public void store(Object key, InternalCacheValue value) {
      delegate.store(key, value);
   }

   public BasicCacheLoader getDelegate() {
      return delegate;
   }


   public static BasicCacheLoader undelegateCacheLoader(BasicCacheLoader store) {
      return store instanceof AbstractDelegatingCacheLoader ? ((AbstractDelegatingCacheLoader)store).getDelegate() : store;
   }
}

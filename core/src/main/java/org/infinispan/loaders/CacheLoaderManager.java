package org.infinispan.loaders;

import java.util.Collection;
import java.util.List;

import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.loaders.decorator.ChainingCacheLoader;
import org.infinispan.loaders.spi.BasicCacheLoader;
import org.infinispan.loaders.spi.BulkCacheLoader;

/**
 * todo
 * 1 - cache loader interceptor
 * 2 - cache store interceptor
 * 3 - make things compile
 * 4 - push pull request
 *
 * The cache loader manager interface
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.0
 */
public interface CacheLoaderManager extends Lifecycle {

   BasicCacheLoader getCacheLoader();

   BulkCacheLoader getBulkCacheLoader();

   boolean isUsingPassivation();

   boolean isShared();

   boolean isFetchPersistentState();

   void preload();

   boolean isEnabled();

   void disableCacheLoader(String loaderType);

   <T extends BasicCacheLoader> List<T> getCacheLoaders(Class<T> loaderClass);

   Collection<String> getCacheLoadersAsString();

   void purgeExpired();
}



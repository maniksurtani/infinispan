package org.infinispan.loaders.spi;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.loaders.CacheStoreConfig;

/**
 * A basic interface to be implemented by the users that want to interpose Infinispan as a cache between an application and
 * a database.
 *
 * DesignNote: this shifts away from the old API which provided two interfaces for storing and loading. Whilst from an
 * OOP design perspective the interface segregation makes sense, it has little benefit in practice: the vast majority
 * of the cache stores are R/W. Also if the user is looking for a read-only store it can provide an empty implementation
 * for store(). OTOH having two concepts (store+loader) has constantly created confusion through the users.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface BasicCacheLoader extends Lifecycle {

   void init(CacheStoreConfig config, Cache cache, StreamingMarshaller m);

   //DesignNote: the InternalCacheValue is the minimum we can use as besides the actual value we also
   // need to store the metadata associated with it
   void store(Object key, InternalCacheValue value);

   InternalCacheValue load(Object key);

   void remove(Object key);

   CacheStoreConfig getCacheLoaderConfig();


}

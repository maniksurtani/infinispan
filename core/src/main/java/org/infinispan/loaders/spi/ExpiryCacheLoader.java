package org.infinispan.loaders.spi;

import java.util.Set;

/**
 * Extension of BasicIterator that brings support for expiry.
 * DesignNote: Keeping purge out of BasicCacheLoader as most of the users wanting to interpose a cache between a DB and
 * app would not think about expiration. BasicCacheLoader should be kept relevant for these users.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface ExpiryCacheLoader extends BasicCacheLoader {

   /**
    * DesignNote: returning the set of keys should offer support for solving https://issues.jboss.org/browse/ISPN-3064
    * (Extend cache expiration notification to cache stores)
    * @return the set of keys that were purged.
    */
   Set purgeExpired();
}

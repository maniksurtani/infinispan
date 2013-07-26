package org.infinispan.loaders.spi;

import org.infinispan.container.entries.InternalCacheValue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface BulkCacheLoader extends BasicCacheLoader {


   EntriesIterator bulkLoad();

   /**
    * DesignNote: this method is to be used in future to optimize preload, i.e. to only load the kys tha map to the
    * local node when the filtering happens based on the consistent has.
    */
   EntriesIterator bulkLoad(KeyFilter filter);

   EntriesIterator bulkLoad(Collection keys);

   KeysIterator bulkLoadKeys(Collection filter);

   void removeAll(Iterator<Object> keys);

   void storeAll(Iterator<Map.Entry<Object, InternalCacheValue>> it);

   void clear();

   int size();

   public interface KeysIterator {

      boolean hasNext();

      /**
       * @return move to the next position and returns the next key
       */
      Object nextKey();

      void close();
   }

   public interface EntriesIterator extends KeysIterator {

      /**
       * DesignNote: use value() instead of Map.Entry to avoid some unnecessary object creation (Map.Entry).
       */
      InternalCacheValue value();
   }


   public interface KeyFilter {

      boolean loadMore();

      boolean shouldLoadKey(Object key);
   }
}

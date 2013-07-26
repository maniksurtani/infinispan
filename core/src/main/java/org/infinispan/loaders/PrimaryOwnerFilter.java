package org.infinispan.loaders;

import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.loaders.spi.BulkCacheLoader;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class PrimaryOwnerFilter implements BulkCacheLoader.KeyFilter {

   private final ClusteringDependentLogic cdl;

   public PrimaryOwnerFilter(ClusteringDependentLogic cdl) {
      this.cdl = cdl;
   }

   @Override
   public boolean loadMore() {
      return true;
   }

   @Override
   public boolean shouldLoadKey(Object key) {
      return cdl.localNodeIsPrimaryOwner(key);
   }
}

package org.infinispan.interceptors;

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.*;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.LoadersConfiguration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.spi.BasicCacheLoader;
import org.infinispan.loaders.spi.BatchingCacheLoader;
import org.infinispan.loaders.spi.BulkCacheLoader;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes modifications back to the store on the way out: stores modifications back through the CacheLoader, either
 * after each method call (no TXs), or at TX commit.
 *
 * Only used for LOCAL and INVALIDATION caches.
 *
 * @author Bela Ban
 * @author Dan Berindei
 * @author Mircea Markus
 * @since 4.0
 */
@MBean(objectName = "CacheStore", description = "Component that handles storing of entries to a CacheStore from memory.")
public class CacheStoreInterceptor extends JmxStatsCommandInterceptor {
   LoadersConfiguration loaderConfig = null;
   final AtomicLong cacheStores = new AtomicLong(0);
   private BasicCacheLoader loader;
   private BulkCacheLoader bulkLoader;
   private CacheLoaderManager loaderManager;
   private InternalEntryFactory entryFactory;
   private TransactionManager transactionManager;
   protected volatile boolean enabled = true;

   private static final Log log = LogFactory.getLog(CacheStoreInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void init(CacheLoaderManager loaderManager, InternalEntryFactory entryFactory, TransactionManager transactionManager) {
      this.loaderManager = loaderManager;
      this.entryFactory = entryFactory;
      this.transactionManager = transactionManager;
      this.loader = loaderManager.getCacheLoader();
   }

   @Start(priority = 15)
   protected void start() {
      loader = loaderManager.getCacheLoader();
      this.setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
      loaderConfig = cacheConfiguration.loaders();
      int concurrencyLevel = cacheConfiguration.locking().concurrencyLevel();
      txStores = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
      preparingTxs = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
   }
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (isStoreEnabled())
         commitCommand(ctx);

      return invokeNextInterceptor(ctx, command);
   }

   protected void commitCommand(TxInvocationContext ctx) throws Throwable {
      if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         // this is a commit call.
         GlobalTransaction tx = ctx.getGlobalTransaction();
         if (getLog().isTraceEnabled()) getLog().tracef("Calling loader.commit() for transaction %s", tx);

         Transaction xaTx = null;
         try {
            xaTx = suspendRunningTx(ctx, xaTx);
            store(ctx);
         } finally {
            resumeRunningTx(xaTx);
         }

         try {
            store.commit(tx);
         } finally {
            // Regardless of outcome, remove from preparing txs
            preparingTxs.remove(tx);

         }
         if (getStatisticsEnabled()) {
            Integer puts = txStores.get(tx);
            if (puts != null) {
               cacheStores.getAndAdd(puts);
            }
            txStores.remove(tx);
         }
      } else {
         if (getLog().isTraceEnabled()) getLog().trace("Commit called with no modifications; ignoring.");
      }
   }

   private void resumeRunningTx(Transaction xaTx) throws InvalidTransactionException, SystemException {
      if (transactionManager != null && xaTx != null) {
         transactionManager.resume(xaTx);
      }
   }

   private Transaction suspendRunningTx(TxInvocationContext ctx, Transaction xaTx) throws SystemException {
      if (transactionManager != null) {
         xaTx = transactionManager.suspend();
         if (xaTx != null && !ctx.isOriginLocal())
            throw new IllegalStateException("It is only possible to be in the context of an JRA transaction in the local node.");
      }
      return xaTx;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (isStoreEnabled()) {
         if (getLog().isTraceEnabled()) getLog().trace("Transactional so don't put stuff in the cache store yet.");
         if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
            GlobalTransaction tx = ctx.getGlobalTransaction();
            // this is a rollback method
            if (preparingTxs.containsKey(tx)) {
               preparingTxs.remove(tx);
               store.rollback(tx);
            }
            if (getStatisticsEnabled()) txStores.remove(tx);
         } else {
            if (getLog().isTraceEnabled()) getLog().trace("Rollback called with no modifications; ignoring.");
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return retval;
      if (!isProperWriter(ctx, command, command.getKey())) return retval;

      Object key = command.getKey();
      boolean resp = store.remove(key);
      if (getLog().isTraceEnabled()) getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      return retval;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope() && isProperWriterForClear(ctx))
         clearCacheStore();

      return invokeNextInterceptor(ctx, command);
   }

   protected void clearCacheStore() throws CacheLoaderException {
      if (bulkLoader != null) {
         bulkLoader.clear();
         if (getLog().isTraceEnabled()) getLog().trace("Cleared cache store");
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;
      if (!isProperWriter(ctx, command, command.getKey())) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      loader.store(se.getKey(), se.toInternalCacheValue());
      if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isStoreEnabled(command) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;
      if (!isProperWriter(ctx, command, command.getKey())) return returnValue;

      Object key = command.getKey();
      InternalCacheEntry se = getStoredEntry(key, ctx);
      loader.store(se.getKey(), se.toInternalCacheValue());
      if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (!isStoreEnabled(command) || ctx.isInTxScope()) return returnValue;

      Map<Object, Object> map = command.getMap();
      for (Object key : map.keySet()) {
         if (isProperWriter(ctx, command, key)) {
            InternalCacheEntry se = getStoredEntry(key, ctx);
            loader.store(se.getKey(), se.toInternalCacheValue());
            if (getLog().isTraceEnabled()) getLog().tracef("Stored entry %s under key %s", se, key);
         }
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(map.size());
      return returnValue;
   }

   protected final void store(TxInvocationContext ctx) throws Throwable {
      List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
      if (modifications.isEmpty()) {
         if (getLog().isTraceEnabled()) getLog().trace("Transaction has not logged any modifications!");
         return;
      }
      if (getLog().isTraceEnabled()) getLog().tracef("Cache loader modification list: %s", modifications);


      Updater modsBuilder = bulkLoader == null ? new Updater(getStatisticsEnabled()) : new BulkUpdater(getStatisticsEnabled());
      for (WriteCommand cacheCommand : modifications) {
         if (isStoreEnabled(cacheCommand)) {
            cacheCommand.acceptVisitor(ctx, modsBuilder);
         }
      }
      modsBuilder.flush();

      if (getStatisticsEnabled() && modsBuilder.putCount > 0) {
         cacheStores.getAndAdd(modsBuilder.putCount);
      }
   }

   protected boolean isStoreEnabled() {
      return enabled;
   }

   protected boolean isStoreEnabled(FlagAffectedCommand command) {
      if (!isStoreEnabled())
         return false;

      if (command.hasFlag(Flag.SKIP_CACHE_STORE)) {
         log.trace("Skipping cache store since the call contain a skip cache store flag");
         return false;
      }
      if (loaderConfig.shared() && command.hasFlag(Flag.SKIP_SHARED_CACHE_STORE)) {
         log.trace("Skipping cache store since it is shared and the call contain a skip shared cache store flag");
         return false;
      }
      return true;
   }

   protected boolean isProperWriter(InvocationContext ctx, FlagAffectedCommand command, Object key) {
      // In invalidation mode we can have remote invalidation commands, and we don't want them to remove
      // entries from a shared cache store.
      return !loaderConfig.shared() || ctx.isOriginLocal();
   }

   protected boolean isProperWriterForClear(InvocationContext ctx) {
      return true;
   }

   public class Updater extends AbstractVisitor {

      protected final boolean generateStatistics;
      int putCount;

      public Updater(boolean generateStatistics) {
         this.generateStatistics = generateStatistics;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return visitSingleStore(ctx, command, command.getKey());
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (isProperWriter(ctx, command, command.getKey())) {
            if (generateStatistics) putCount++;
            CacheEntry entry = ctx.lookupEntry(command.getKey());
            InternalCacheEntry ice;
            if (entry instanceof InternalCacheEntry) {
               ice = (InternalCacheEntry) entry;
            } else if (entry instanceof DeltaAwareCacheEntry) {
               AtomicHashMap<?,?> uncommittedChanges = ((DeltaAwareCacheEntry) entry).getUncommittedChages();
               ice = entryFactory.create(entry.getKey(), uncommittedChanges, entry.getMetadata(), entry.getLifespan(), entry.getMaxIdle());
            } else {
               ice = entryFactory.create(entry);
            }
            loader.store(ice.getKey(), ice.toInternalCacheValue());
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return visitSingleStore(ctx, command, command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> map = command.getMap();
         for (Object key : map.keySet())
            visitSingleStore(ctx, command, key);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         Object key = command.getKey();
         if (isProperWriter(ctx, command, key)) {
            loader.remove(key);
         }
         return null;
      }

      protected Object visitSingleStore(InvocationContext ctx, FlagAffectedCommand command, Object key) throws Throwable {
         if (isProperWriter(ctx, command, key)) {
            if (generateStatistics) putCount++;
            loader.store(key, getStoredValue(key, ctx));
         }
         return null;
      }

      protected void flush() {
         //no op
      }
   }

   public class BulkUpdater extends Updater {

      List<Object> keysToRemove;
      Map<Object, InternalCacheValue> entriesToAdd;

      public BulkUpdater(boolean generateStatistics) {
         super(generateStatistics);
      }

      protected Object visitSingleStore(InvocationContext ctx, FlagAffectedCommand command, Object key) throws Throwable {
         if (isProperWriter(ctx, command, key)) {
            if (generateStatistics) putCount++;
            if (entriesToAdd == null)
               entriesToAdd = new HashMap<Object, InternalCacheValue>();
            entriesToAdd.put(key, getStoredValue(key, ctx));
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (isProperWriter(ctx, command, command.getKey())) {
            if (keysToRemove == null) keysToRemove = new ArrayList<Object>();
            keysToRemove.add(command.getKey());
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         if (isProperWriterForClear(ctx))
            bulkLoader.clear();
         return null;
      }

      @Override
      protected void flush() {
         if (keysToRemove != null)
            bulkLoader.removeAll(keysToRemove.iterator());
         if (entriesToAdd != null)
            bulkLoader.storeAll(entriesToAdd.entrySet().iterator());
      }
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics"
   )
   public void resetStatistics() {
      cacheStores.set(0);
   }

   @ManagedAttribute(
         description = "number of cache loader stores",
         displayName = "Number of cache stores",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderStores() {
      return cacheStores.get();
   }

   InternalCacheValue getStoredValue(Object key, InvocationContext ctx) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry instanceof InternalCacheEntry) {
         return ((InternalCacheEntry) entry).toInternalCacheValue();
      } else {
         if (ctx.isInTxScope()) {
            EntryVersionsMap updatedVersions =
                  ((TxInvocationContext) ctx).getCacheTransaction().getUpdatedEntryVersions();
            if (updatedVersions != null) {
               EntryVersion version = updatedVersions.get(entry.getKey());
               if (version != null) {
                  Metadata metadata = entry.getMetadata();
                  if (metadata == null) {
                     // If no metadata passed, assumed embedded metadata
                     metadata = new EmbeddedMetadata.Builder()
                           .lifespan(entry.getLifespan()).maxIdle(entry.getMaxIdle())
                           .version(version).build();
                     return entryFactory.create(entry.getKey(), entry.getValue(), metadata).toInternalCacheValue();
                  } else {
                     metadata = metadata.builder().version(version).build();
                     return entryFactory.create(entry.getKey(), entry.getValue(), metadata).toInternalCacheValue();
                  }
               }
            }
         }

         return entryFactory.create(entry).toInternalCacheValue();
      }
   }

   public void disableInterceptor() {
      enabled = false;
   }

   public Map<GlobalTransaction, Set<Object>> getPreparingTxs() {
      return Collections.unmodifiableMap(preparingTxs);
   }

   public Map<GlobalTransaction, Integer> getTxStores() {
      return Collections.unmodifiableMap(txStores);
   }
}

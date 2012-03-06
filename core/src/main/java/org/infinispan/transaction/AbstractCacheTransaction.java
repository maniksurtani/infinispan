/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.customcollections.CustomCollections;
import org.infinispan.util.customcollections.CacheEntryCollection;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;
import org.infinispan.util.customcollections.ModificationCollection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for local and remote transaction. Impl note: The aggregated modification list and lookedUpEntries are not
 * instantiated here but in subclasses. This is done in order to take advantage of the fact that, for remote
 * transactions we already know the size of the modifications list at creation time.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.2
 */
public abstract class AbstractCacheTransaction implements CacheTransaction {

   protected final GlobalTransaction tx;
   private static Log log = LogFactory.getLog(AbstractCacheTransaction.class);
   private static final boolean trace = log.isTraceEnabled();
<<<<<<< Updated upstream
   private static final int INITIAL_LOCK_CAPACITY = 4;
=======
>>>>>>> Stashed changes

   protected ModificationCollection modifications;
   protected CacheEntryCollection lookedUpEntries;
   protected KeyCollection affectedKeys = null;
   protected KeyCollection lockedKeys;
   protected KeyCollection backupKeyLocks = null;
   private boolean txComplete = false;
   protected volatile boolean prepared;
   private volatile boolean needToNotifyWaiters = false;
   final int viewId;

   private EntryVersionsMap updatedEntryVersions;

   public AbstractCacheTransaction(GlobalTransaction tx, int viewId) {
      this.tx = tx;
      this.viewId = viewId;
   }

   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   public ModificationCollection getModifications() {
      return CustomCollections.nullCheck(modifications);
   }

   public void setModifications(ModificationCollection modifications) {
      this.modifications = modifications;
   }

   @Override
   public CacheEntryCollection getLookedUpEntries() {
      return CustomCollections.nullCheck(lookedUpEntries);
   }

   public CacheEntry lookupEntry(Object key) {
      return lookedUpEntries == null ? null : lookedUpEntries.findCacheEntry(key);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries = null;
   }

   @Override
   public boolean ownsLock(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public void notifyOnTransactionFinished() {
      if (trace) log.tracef("Transaction %s has completed, notifying listening threads.", tx);
      txComplete = true; //this one is cheap but does not guarantee visibility
      if (needToNotifyWaiters) {
         synchronized (this) {
            txComplete = true; //in this case we want to guarantee visibility to other threads
            this.notifyAll();
         }
      }
   }

   @Override
   public boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException {
      if (txComplete) return true; //using an unsafe optimisation: if it's true, we for sure have the latest read of the value without needing memory barriers
      final boolean potentiallyLocked = hasLockOrIsLockBackup(key);
      if (trace) log.tracef("Transaction gtx=%s potentially locks key %s? %s", tx, key, potentiallyLocked);
      if (potentiallyLocked) {
         synchronized (this) {
            // Check again after acquiring a lock on the monitor that the transaction has completed.
            // If it has completed, all of its locks would have been released.
            needToNotifyWaiters = true;
            //The order in which these booleans are verified is critical as we take advantage of it to avoid otherwise needed locking
            if (txComplete) {
               needToNotifyWaiters = false;
               return true;
            }
            this.wait(lockAcquisitionTimeout);

            // Check again in case of spurious thread signalling
            return txComplete;
         }
      }
      return true;
   }

   @Override
   public int getViewId() {
      return viewId;
   }

   @Override
   public void addBackupLockForKey(Object key) {
      if (backupKeyLocks == null)
         backupKeyLocks = new KeyCollectionImpl(key);
      else
         backupKeyLocks.add(key);
   }

   public void registerLockedKey(Object key) {
<<<<<<< Updated upstream
      if (lockedKeys == null) lockedKeys = new HashSet<Object>(INITIAL_LOCK_CAPACITY);
      if (trace) log.tracef("Registering locked key: %s", key);
      lockedKeys.add(key);
=======
      if (lockedKeys == null)
         lockedKeys = new KeyCollectionImpl(key);
      else
         lockedKeys.add(key);
>>>>>>> Stashed changes
   }

   public KeyCollection getLockedKeys() {
      return CustomCollections.nullCheck(lockedKeys);
   }

   public void clearLockedKeys() {
      if (trace) log.tracef("Clearing locked keys: %s", lockedKeys);
      lockedKeys = null;
   }

   private boolean hasLockOrIsLockBackup(Object key) {
      return (lockedKeys != null && lockedKeys.contains(key)) || (backupKeyLocks != null && backupKeyLocks.contains(key));
   }

   public KeyCollection getAffectedKeys() {
      return CustomCollections.nullCheck(affectedKeys);
   }

   public void addAffectedKey(Object key) {
      if (affectedKeys == null)
         affectedKeys = new KeyCollectionImpl(key);
      else
         affectedKeys.add(key);
   }

   public void addAllAffectedKeys(KeyCollection keys) {
      if (affectedKeys == null)
         affectedKeys = keys.clone();
      else
         affectedKeys.addAll(keys);
   }

   @Override
   public EntryVersionsMap getUpdatedEntryVersions() {
      return updatedEntryVersions;
   }

   @Override
   public void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions) {
      this.updatedEntryVersions = updatedEntryVersions;
   }
}

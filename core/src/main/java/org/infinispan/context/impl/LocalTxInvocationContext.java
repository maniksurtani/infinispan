/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.AbstractCacheTransaction;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.customcollections.CacheEntryCollection;
import org.infinispan.util.customcollections.CacheEntryCollectionImpl;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;
import org.infinispan.util.customcollections.ModificationCollection;
import org.infinispan.util.customcollections.ModificationCollectionImpl;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Collection;

/**
 * Invocation context to be used for locally originated transactions.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class LocalTxInvocationContext extends AbstractTxInvocationContext {

   private LocalTransaction localTransaction;

   public boolean isTransactionValid() {
      Transaction t = getTransaction();
      int status = -1;
      if (t != null) {
         try {
            status = t.getStatus();
         } catch (SystemException e) {
            // no op
         }
      }
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   public boolean isOriginLocal() {
      return true;
   }

   public Object getLockOwner() {
      return localTransaction.getGlobalTransaction();
   }

   public GlobalTransaction getGlobalTransaction() {
      return localTransaction.getGlobalTransaction();
   }

   @Override
   public ModificationCollection getModifications() {
      return localTransaction == null ? ModificationCollectionImpl.EMPTY_MODIFICATION_COLLECTION : localTransaction.getModifications();
   }

   public void setLocalTransaction(LocalTransaction localTransaction) {
      this.localTransaction = localTransaction;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      return localTransaction != null ? localTransaction.lookupEntry(key) : null;
   }

   @Override
   public CacheEntryCollection getLookedUpEntries() {
      return localTransaction == null ? CacheEntryCollectionImpl.EMPTY_CACHE_ENTRY_COLLECTION : localTransaction.getLookedUpEntries();
   }

   @Override
   public void putLookedUpEntry(CacheEntry e) {
      localTransaction.putLookedUpEntry(e);
   }

   public void clearLookedUpEntries() {
      localTransaction.clearLookedUpEntries();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return localTransaction != null && localTransaction.ownsLock(key);
   }

   public void remoteLocksAcquired(Collection<Address> nodes) {
      localTransaction.locksAcquired(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired() {
      return localTransaction.getRemoteLocksAcquired();
   }

   @Override
   public AbstractCacheTransaction getCacheTransaction() {
      return localTransaction;
   }

   @Override
   public KeyCollection getLockedKeys() {
      return localTransaction == null ? KeyCollectionImpl.EMPTY_KEY_COLLECTION : localTransaction.getLockedKeys();
   }

   @Override
   public void addLockedKey(Object key) {
      localTransaction.registerLockedKey(key);
   }

   public Transaction getTransaction() {
      Transaction tx = super.getTransaction();
      return tx == null ? localTransaction.getTransaction() : tx;
   }
}

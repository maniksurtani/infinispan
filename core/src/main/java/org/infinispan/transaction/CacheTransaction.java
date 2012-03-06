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
package org.infinispan.transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.EntryLookup;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.customcollections.ModificationCollection;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines the state a infinispan transaction should have.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheTransaction extends EntryLookup {

   /**
    * Returns the transaction identifier.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications visible within the current transaction.
    */
   ModificationCollection getModifications();

   abstract boolean ownsLock(Object key);
   
   void clearLockedKeys();

   public Set<Object> getLockedKeys();

   int getViewId();

   void addBackupLockForKey(Object key);

   /**
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   void notifyOnTransactionFinished();

   /**
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException;

   // TODO: MS: return an array here?  Assume guarantees that this array will always contain unique entries, like a set.
   EntryVersionsMap getUpdatedEntryVersions();

   void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions);
}

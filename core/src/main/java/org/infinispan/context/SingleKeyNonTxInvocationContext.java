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

package org.infinispan.context;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.AbstractInvocationContext;
import org.infinispan.util.customcollections.CacheEntryCollection;
import org.infinispan.util.customcollections.CacheEntryCollectionImpl;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;

/**
 * @author Mircea Markus
 * @since 5.1
 */
public class SingleKeyNonTxInvocationContext extends AbstractInvocationContext {

   private final boolean isOriginLocal;

   private CacheEntry cacheEntry;
   private Object lockedKey;
   private int keyHashCode;

   public SingleKeyNonTxInvocationContext(boolean originLocal) {
      isOriginLocal = originLocal;
   }

   @Override
   public boolean isOriginLocal() {
      return isOriginLocal;
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return Thread.currentThread();
   }

   @Override
   public KeyCollection getLockedKeys() {
      return lockedKey == null ? KeyCollectionImpl.EMPTY_KEY_COLLECTION : KeyCollectionImpl.singleton(lockedKey);
   }

   @Override
   public void clearLockedKeys() {
      cacheEntry = null;
   }

   @Override
   public void addLockedKey(Object key) {
      if (cacheEntry != null && !key.equals(cacheEntry.getKey()))
         throw illegalStateException();
      lockedKey = key;
   }

   private IllegalStateException illegalStateException() {
      return new IllegalStateException("This is a single key invocation context, using multiple keys shouldn't be possible");
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      if (key != null && cacheEntry != null && key.hashCode() == getKeyHashCode() && key.equals(cacheEntry.getKey())) return cacheEntry;
      return null;
   }

   private int getKeyHashCode() {
      if (keyHashCode == -1) {
         keyHashCode = cacheEntry.getKey().hashCode();
      }
      return keyHashCode;
   }
   
   @Override
   public CacheEntryCollection getLookedUpEntries() {
      return cacheEntry == null ? CacheEntryCollectionImpl.EMPTY_CACHE_ENTRY_COLLECTION : CacheEntryCollectionImpl.singleton(cacheEntry);
   }

   @Override
   public void putLookedUpEntry(CacheEntry e) {
      this.cacheEntry = e;
   }

   @Override
   public void clearLookedUpEntries() {
      clearLockedKeys();
   }

   public CacheEntry getCacheEntry() {
      return cacheEntry;
   }
}

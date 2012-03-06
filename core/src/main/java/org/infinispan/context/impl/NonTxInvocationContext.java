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
import org.infinispan.util.customcollections.CustomCollections;
import org.infinispan.util.customcollections.CacheEntryCollection;
import org.infinispan.util.customcollections.CacheEntryCollectionImpl;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   private CacheEntryCollection lookedUpEntries;
   private KeyCollection lockedKeys;

   public NonTxInvocationContext(int numEntries, boolean local) {
      lookedUpEntries = new CacheEntryCollectionImpl(numEntries);
      setOriginLocal(local);
   }

   public NonTxInvocationContext() {
   }

   @Override
   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries.findCacheEntry(k);
   }

   @Override
   public void putLookedUpEntry(CacheEntry e) {
      lookedUpEntries.add(e);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   public CacheEntryCollection getLookedUpEntries() {
      return lookedUpEntries;
   }

   public boolean isOriginLocal() {
      return isContextFlagSet(ContextFlag.ORIGIN_LOCAL);
   }

   public void setOriginLocal(boolean originLocal) {
      setContextFlag(ContextFlag.ORIGIN_LOCAL, originLocal);
   }

   public boolean isInTxScope() {
      return false;
   }

   public Object getLockOwner() {
      return Thread.currentThread();
   }

   @Override
   public void reset() {
      super.reset();
      clearLookedUpEntries();
      if (lockedKeys != null) lockedKeys = null;
   }

   @Override
   public NonTxInvocationContext clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      dolly.lookedUpEntries = lookedUpEntries.clone();
      if (lockedKeys != null) dolly.lockedKeys = lockedKeys.clone();
      return dolly;
   }

   @Override
   public void addLockedKey(Object key) {
      if (lockedKeys == null)
         lockedKeys = new KeyCollectionImpl(key);
      else
         lockedKeys.add(key);
   }

   @Override
   public KeyCollection getLockedKeys() {
      return CustomCollections.nullCheck(lockedKeys);
   }

   @Override
   public void clearLockedKeys() {
      lockedKeys = null;
   }
}

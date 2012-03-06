/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.util.customcollections;

import org.infinispan.container.entries.CacheEntry;

import java.util.Arrays;
import java.util.Iterator;

/**
 * // TODO: MS: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class CacheEntryCollectionImpl implements CacheEntryCollection {
   public static final CacheEntryCollection EMPTY_CACHE_ENTRY_COLLECTION = new CacheEntryCollectionImpl();

   CacheEntry[] entries;

   private CacheEntryCollectionImpl() {
   }

   public CacheEntryCollectionImpl(int sizingHint) {
      // TODO: MS: do something with sizingHint!
   }

   public CacheEntryCollectionImpl(CacheEntry e) {
      entries = new CacheEntry[]{e};
   }

   @Override
   public boolean contains(CacheEntry entry) {
      return CustomCollections.contains(entries, entry);
   }

   @Override
   public boolean isEmpty() {
      return entries.length == 0;
   }

   @Override
   public int size() {
      return entries.length;
   }

   @Override
   public void add(CacheEntry entry) {
      entries = CustomCollections.addToArray(entries, entry, true);
   }

   @Override
   public CacheEntry findCacheEntry(Object key) {
      return CustomCollections.findCacheEntry(entries, key);
   }

   @Override
   public void clear() {
      entries = null;
   }

   @Override
   public Iterator<CacheEntry> iterator() {
      return new Iterator<CacheEntry>() {
         int idx = 0;
         @Override
         public boolean hasNext() {
            return idx < entries.length;
         }

         @Override
         public CacheEntry next() {
            return entries[idx++];
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      };
   }

   @Override
   public CacheEntryCollection clone() {
      CacheEntryCollectionImpl dolly = new CacheEntryCollectionImpl(entries.length);
      if (entries != null) dolly.entries = entries.clone();
      return dolly;
   }

   @Override
   public String toString() {
      return "CacheEntryCollection{" +
            "" + (entries == null ? null : Arrays.toString(entries)) +
            '}';
   }

   public static CacheEntryCollection singleton(CacheEntry cacheEntry) {
      // TODO: MS: can we do better?
      return new CacheEntryCollectionImpl(cacheEntry);
   }
}

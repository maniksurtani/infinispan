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

import org.infinispan.util.TimSort;
import org.infinispan.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * // TODO: MS: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class KeyCollectionImpl implements KeyCollection {
   public static final KeyCollection EMPTY_KEY_COLLECTION = new KeyCollectionImpl();

   Object[] keys;

   private KeyCollectionImpl() {
   }

   public KeyCollectionImpl(int sizeHint, boolean maintainUniqueness) {
      // TODO: MS: make use of these settings
   }

   public KeyCollectionImpl(Object initKey) {
      keys = new Object[]{initKey};
   }

   public KeyCollectionImpl(Collection<Object> initKeys) {
      if (initKeys != null && !initKeys.isEmpty()) {
         keys = new Object[initKeys.size()];
         int i=0;
         for (Object k: initKeys) keys[i++] = k;
      }
   }

   @Override
   public boolean contains(Object key) {
      return CustomCollections.contains(keys, key);
   }

   @Override
   public boolean isEmpty() {
      return keys.length == 0;
   }

   @Override
   public int size() {
      return keys.length;
   }

   @Override
   public void add(Object key) {
      keys = CustomCollections.addToArray(keys, key, true);
   }

   @Override
   public void addAll(KeyCollection otherKeys) {
      keys = CustomCollections.addToArray(keys, ((KeyCollectionImpl)otherKeys).keys, true);
   }

   @Override
   public KeyCollection clone() {
      KeyCollectionImpl dolly = new KeyCollectionImpl();
      if (keys != null) dolly.keys = keys.clone();
      return dolly;
   }

   @Override
   public boolean containsAny(KeyCollection otherKeys) {
      return CustomCollections.containsAny(keys, ((KeyCollectionImpl) otherKeys).keys);
   }

   @Override
   public Object getFirst() {
      return keys == null || keys.length == 0 ? null : keys[0];
   }
   
   @Override
   public Object[] toArray() {
      return keys == null ? Util.EMPTY_OBJECT_ARRAY : keys;
   }
   
   public void remove(Object key) {
      // TODO: MS: Implement!!
      throw new UnsupportedOperationException("IMPLEMENT ME");

   }

   @Override
   public void sort(Comparator<Object> keyComparator) {
      if (keys != null && keys.length > 1) TimSort.sort(keys, keyComparator);
   }

   @Override
   public boolean containsAll(KeyCollection otherKeys) {
      return CustomCollections.containsAll(this.keys, ((KeyCollectionImpl) otherKeys).keys);
   }

   @Override
   public Set<Object> asSet() {
      // TODO: MS: optimise this!
      Set<Object> s = new HashSet<Object>();
      for (Object k: keys) s.add(k);
      return s;
   }

   @Override
   public Iterator<Object> iterator() {
      return new Iterator<Object>() {
         int idx = 0;
         @Override
         public boolean hasNext() {
            return idx < keys.length;
         }

         @Override
         public Object next() {
            return keys[idx++];
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      };
   }

   @Override
   public String toString() {
      return "KeyCollection{" +
            "" + (keys == null ? null : Arrays.toString(keys)) +
            '}';
   }

   public static KeyCollection singleton(Object key) {
      // TODO: MS: can we do better?
      return new KeyCollectionImpl(key);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyCollectionImpl objects = (KeyCollectionImpl) o;

      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      if (!Arrays.equals(keys, objects.keys)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return keys != null ? Arrays.hashCode(keys) : 0;
   }

   public static KeyCollection fromArray(Object... keys) {
      KeyCollectionImpl kci = new KeyCollectionImpl();
      kci.keys = keys;
      return kci;
   }

   public static KeyCollection fromCollection(Collection<?> keys) {
      // TODO: MS: implement this better!
      return fromArray(keys.toArray());
   }
}

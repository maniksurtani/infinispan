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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.Util;
import org.infinispan.util.customcollections.CacheEntryCollection;
import org.infinispan.util.customcollections.CacheEntryCollectionImpl;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;
import org.infinispan.util.customcollections.ModificationCollection;
import org.infinispan.util.customcollections.ModificationCollectionImpl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Helpers that manipulate custom collections used within contexts and CacheTransaction implementations.
 *
 * @author Manik Surtani
 * @since 5.2
 */
public class CustomCollections {

   static CacheEntry findCacheEntry(CacheEntry[] haystack, Object needle) {
      if (isEmpty(haystack)) return null;
      // TODO: MS: very very inefficient!!!
      int kHash = needle.hashCode();
      for (CacheEntry ce: haystack) {
         if (ce != null && ce.getKey().hashCode() == kHash && ce.getKey().equals(needle))
            return ce;
      }
      return null;
   }

   static Object[] addToArray(Object[] array, Object newValue, boolean maintainUniqueness) {
      // TODO: MS: very very inefficient!!!
      if (isEmpty(array)) {
         return new Object[]{newValue};
      } else {
         Object[] copy = Arrays.copyOf(array, array.length + 1);
         copy[array.length] = newValue;
         if (maintainUniqueness) copy = dedupe(copy);
         return copy;
      }
   }

   static Object[] addToArray(Object[] array, Object[] newValue, boolean maintainUniqueness) {
      // TODO: MS: very very inefficient!!!
      if (isEmpty(array)) {
         if (maintainUniqueness) newValue = dedupe(newValue);
         return newValue;
      } else {
         Object[] copy = Arrays.copyOf(array, array.length + newValue.length);
         System.arraycopy(newValue, 0, copy, array.length, newValue.length);
         if (maintainUniqueness) copy = dedupe(copy);
         return copy;
      }
   }

   private static Object[] dedupe(Object[] array) {
      // TODO: MS: WTF?!?
      Set<Object> s = new HashSet<Object>(array.length);
      Collections.addAll(s, array);
      return s.toArray();
   }
   
   
   static WriteCommand[] addToArray(WriteCommand[] array, WriteCommand newValue) {
      // TODO: MS: very very inefficient!!!
      if (isEmpty(array)) {
         return new WriteCommand[]{newValue};
      } else {
         // Do this manually.  Arrays.copyOf() will use Reflection!!
         WriteCommand[] copy = new WriteCommand[array.length + 1];
         System.arraycopy(array, 0, copy, 0, array.length);
         copy[array.length] = newValue;
         return copy;
      }
   }

   static CacheEntry[] addToArray(CacheEntry[] array, CacheEntry newValue, boolean maintainUniqueness) {
      // TODO: MS: very very inefficient!!!

      if (isEmpty(array)) {
         return new CacheEntry[]{newValue};
      } else {
         if (maintainUniqueness && replace(array, newValue.getKey(), newValue)) return array;

         // Do this manually.  Arrays.copyOf() will use Reflection!!
         CacheEntry[] copy = new CacheEntry[array.length + 1];
         System.arraycopy(array, 0, copy, 0, array.length);
         copy[array.length] = newValue;
         return copy;
      }
   }

   static boolean replace(CacheEntry[] array, Object keyToReplace, CacheEntry newValue) {
      int khash = keyToReplace.hashCode();
      for (int i = 0; i<array.length; i++) {
         CacheEntry ce = array[i];
         if (ce.getKey().hashCode() == khash && ce.getKey().equals(keyToReplace)) {
            array[i] = newValue;
            return true;
         }
      }
      return false;
   }

   static <T> boolean contains(T[] haystack, T needle) {
      // TODO: MS: very very inefficient!!!
      if (!isEmpty(haystack)) {
         for (T t: haystack) {
            if (Util.safeEquals(t, needle)) return true;
         }
      }
      return false;
   }

   static <T> boolean containsAll(T[] haystack, T[] needles) {
      if (isEmpty(needles) || isEmpty(haystack)) return false;
      // TODO: MS: very very inefficient!!!
      boolean winning = true;
      for (T needle: needles) {
         if (!winning) return false;
         winning = contains(haystack, needle);
      }
      return winning;
   }

   static <T> boolean containsAny(T[] haystack, T[] needles) {
      if (isEmpty(needles) || isEmpty(haystack)) return false;
      for (Object needle: needles) {
         if (contains(haystack, needle)) return true;
      }
      return false;
   }

   public static <T> boolean isEmpty(T[] array) {
      return array == null || array.length == 0;
   }
   
   public static CacheEntryCollection nullCheck(CacheEntryCollection coll) {
      return coll == null ? CacheEntryCollectionImpl.EMPTY_CACHE_ENTRY_COLLECTION : coll;
   }

   public static KeyCollection nullCheck(KeyCollection coll) {
      return coll == null ? KeyCollectionImpl.EMPTY_KEY_COLLECTION : coll;
   }

   public static ModificationCollection nullCheck(ModificationCollection coll) {
      return coll == null ? ModificationCollectionImpl.EMPTY_MODIFICATION_COLLECTION : coll;
   }

   /**
    * A function that converts an entry into a key/value pair for use in a map.
    * @param <K> generated key
    * @param <V> generated value
    * @param <E> entry input
    */
   public static interface MapMakerFunction<K, V, E> {
      /**
       * Transforms the given input into a key/value pair for use in a map
       * @param input instance of the input type
       * @return a Map.Entry parameterized with K and V
       */
      Map.Entry<K, V> transform(E input);
   }

   /**
    * Given a collection, transforms the collection to a map given a {@link MapMakerFunction}
    *
    * @param input contains a collection of type E
    * @param f MapMakerFunction instance to use to transform the collection to a key/value pair
    * @param <K> output map's key type
    * @param <V> output type of the map's value
    * @param <E> input collection's entry type
    * @return a Map with keys and values generated from the input collection
    */
   public static <K, V, E> Map<K, V> transformCollectionToMap(Collection<E> input, MapMakerFunction<K, V, E> f) {
      // This screams for a map function! Gimme functional programming pleasee...
      if (input.isEmpty()) return Collections.emptyMap();
      if (input.size() == 1) {
         E single = input.iterator().next();
         Map.Entry<K, V> entry = f.transform(single);
         return singletonMap(entry.getKey(), entry.getValue());
      } else {
         Map<K, V> map = new HashMap<K, V>(input.size());
         for (E e : input) {
            Map.Entry<K, V> entry = f.transform(e);
            map.put(entry.getKey(), entry.getValue());
         }
         return unmodifiableMap(map);
      }
   }
}

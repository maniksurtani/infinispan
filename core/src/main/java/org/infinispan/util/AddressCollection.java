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

package org.infinispan.util;

import org.infinispan.remoting.transport.Address;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class AddressCollection implements Iterable<Address>, Cloneable {
   private final Address[] view;
   private final BitSet elements;
   private int size;
   
   AddressCollection(Address[] view) {
      this.view = view;
      this.elements = new BitSet();
      for (int i=0; i<view.length; i++) this.elements.set(i, false);
   }
   
   
   public void remove(Address toRemove) {
      for (int i=0; i<view.length; i++) {
         if (view[i] == toRemove) {
            elements.set(i,false);
            break;
         }
      }
   }

   public Address remove(int i) {
      elements.set(i, false);
      return view[i];
   }

   public int size() {
      return size;
   }

   public void retainAll(AddressCollection other) {

   }

   @Override
   public Iterator<Address> iterator() {
      return new Iterator<Address>() {

         @Override
         public boolean hasNext() {
            return false;  // TODO: Customise this generated block
         }

         @Override
         public Address next() {
            return null;  // TODO: Customise this generated block
         }

         @Override
         public void remove() {
            // TODO: Customise this generated block
         }
      };
   }

   public boolean contains(Address self) {
      
      return false;

   }

   public void addAll(AddressCollection addresses) {
      
   }

   public Address getFirst() {
      return get(0);
   }

   public void add(Address value) {
      // TODO: Customise this generated block
   }

   public AddressCollection clone() {
      try {
         return (AddressCollection) super.clone();
      } catch (CloneNotSupportedException e) {
         // should never happen
         return null;
      }
   }

   public boolean isEmpty() {
      return size == 0;
   }

   public boolean containsAll(AddressCollection recipients) {
      return false;  // TODO: Customise this generated block
   }

   public boolean containsAll(Address... recipients) {
      return false;  // TODO: Customise this generated block
   }

   public AddressCollection subCollection(int i, int size) {
      return null;  // TODO: Customise this generated block
   }

   public void removeAll(AddressCollection oldList) {
      // TODO: Customise this generated block
   }

   public Address get(int i) {
      return null;  // TODO: Customise this generated block
   }

   public void addAll(Collection<Address> requestorAddresses) {
      // TODO: Customise this generated block
   }

   public void clear() {



   }

   public void addAll(Address[] moreAddresses) {
      // TODO: Customise this generated block
   }

   public Address[] toArray(Address[] addresses) {
      return new Address[0];  // TODO: Customise this generated block
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AddressCollection addresses = (AddressCollection) o;

      return true;
   }

   @Override
   public int hashCode() {
      return 11;
   }

   @Override
   public String toString() {
      return ":";
   }
}

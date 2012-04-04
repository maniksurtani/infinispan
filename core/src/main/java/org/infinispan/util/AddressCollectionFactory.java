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

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class AddressCollectionFactory {

   Map<Integer, Address[]> views;
   
   public static AddressCollection allAddresses(Address... addresses) {
      return new AddressCollection(null);
   }

   public static AddressCollection allAddresses() {
      return new AddressCollection(null);
   }

   public static AddressCollection allAddressesExcept(Address... addresses) {
      return new AddressCollection(null);
   }

   public static AddressCollection emptyCollection() {
      return new AddressCollection(null);
   }

   public static AddressCollection singleton(Address physicalAddress) {
      return null;  // TODO: Customise this generated block
   }

   public static List<Address> toList(AddressCollection members) {
      return null;  // TODO: Customise this generated block
   }

   public static List<List<Address>> toListOfLists(List<AddressCollection> subgroupsMerged) {
      return null;  // TODO: Customise this generated block
   }

   public static AddressCollection fromCollection(Collection<Address> newMembers) {
      return null;  // TODO: Customise this generated block

   }
}

/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.container.versioning;

import net.jcip.annotations.Immutable;
import org.infinispan.remoting.transport.Address;

/**
 * The counter maintained by a single node
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Immutable
public class NodeClock {
   private final Address nodeAddress;
   private final long counter;

   public NodeClock(Address nodeAddress, long counter) {
      this.nodeAddress = nodeAddress;
      if (counter < 1) throw new IllegalArgumentException(String.format("Version for node %s needs to be greater than 0.", nodeAddress));
      this.counter = counter;
   }

   public NodeClock increment() {
      return new NodeClock(nodeAddress, counter + 1);
   }

   public long getCounter() {
      return counter;
   }

   public Address getAddress() {
      return nodeAddress;
   }
}

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

import org.infinispan.remoting.transport.Address;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * An implementation of a vector clock to track concurrently updatable versions.
 *
 * @author Manik Surtani
 * @see <a href="http://en.wikipedia.org/wiki/Vector_clock">Wikipedia's article on vector clocks</a>.
 * @since 5.1
 */
public class VectorClockEntryVersion implements EntryVersion {
   private final SortedMap<Address, NodeClock> clocks;
   private long timestamp;
   private static final AddressComparator cmp = new AddressComparator();

//   public VectorClockEntryVersion(Collection<NodeClock> clocks, long timestamp) {
//      this.clocks = new TreeMap<Address, NodeClock>(cmp);
//      for (NodeClock nc : clocks) this.clocks.put(nc.getAddress(), nc);
//      this.timestamp = timestamp;
//   }
//
//   public VectorClockEntryVersion(Collection<NodeClock> clocks) {
//      this(clocks, System.currentTimeMillis());
//   }

   public VectorClockEntryVersion(Address originator) {
      this.clocks = new TreeMap<Address, NodeClock>(cmp);
      this.timestamp = System.currentTimeMillis();
      this.clocks.put(originator, new NodeClock(originator, 1));
   }

   public VectorClockEntryVersion increment(Object incrementorAddress) {
      this.timestamp = System.currentTimeMillis();
      Address incrementor = (Address) incrementorAddress;
      NodeClock nc = clocks.get(incrementor);
      if (nc == null)
         nc = new NodeClock(incrementor, 1);
      else
         nc = nc.increment();
      clocks.put(incrementor, nc);
      return this;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion v) {
      if (!(v instanceof VectorClockEntryVersion))
         throw new IllegalArgumentException("Cannot compare Versions of different types.");

      return compare(this, (VectorClockEntryVersion) v);
   }

   /**
    * TODO needs careful testing
    *
    * @param v1 The first VectorClockEntryVersion
    * @param v2 The second VectorClockEntryVersion
    */
   public static InequalVersionComparisonResult compare(VectorClockEntryVersion v1, VectorClockEntryVersion v2) {
      // TODO This is work in progress

      if (v1 == null || v2 == null) throw new NullPointerException("Null arguments not allowed");
      // We do two checks: v1 <= v2 and v2 <= v1 if both are true then
      boolean v1Newer = false;
      boolean v2Newer = false;
      boolean progressV1 = true, progressV2 = true;
      NodeClock ver1 = null, ver2 = null;

      Iterator<NodeClock> v1Iterator = v1.clocks.values().iterator();
      Iterator<NodeClock> v2Iterator = v2.clocks.values().iterator();


      while (v1Iterator.hasNext() && v2Iterator.hasNext()) {
         if (progressV1) {
            ver1 = v1Iterator.next();
            progressV1 = false;
         }
         if (progressV2) {
            ver2 = v2Iterator.next();
            progressV2 = false;
         }
         if (ver1.getAddress().equals(ver2.getAddress())) {
            if (ver1.getCounter() > ver2.getCounter())
               v1Newer = true;
            else if (ver2.getCounter() > ver1.getCounter())
               v2Newer = true;
            progressV1 = true;
            progressV2 = true;
         } else if (cmp.compare(ver1.getAddress(), ver2.getAddress()) > 0) {
            v2Newer = true;
            progressV2 = true;
         } else {
            v1Newer = true;
            progressV1 = true;
         }
      }

      /* Okay, now check for left overs */
      if (v1Iterator.hasNext())
         v1Newer = true;
      else if (v2Iterator.hasNext())
         v2Newer = true;

      /* This is the case where they are equal, return BEFORE arbitrarily */
      if (!v1Newer && !v2Newer)
         return BEFORE;
         /* This is the case where v1 is a successor clock to v2 */
      else if (v1Newer && !v2Newer)
         return AFTER;
         /* This is the case where v2 is a successor clock to v1 */
      else if (!v1Newer && v2Newer)
         return BEFORE;
         /* This is the case where both clocks are parallel to one another */
      else
         return CONFLICTING;
   }

   @Override
   public boolean equals(Object other) {
      if (other instanceof VectorClockEntryVersion)
         return clocks.equals(((VectorClockEntryVersion) other).clocks);
      else
         return false;
   }
}

class AddressComparator implements Comparator<Address> {

   @Override
   public int compare(Address address, Address address1) {
      // TODO use a bitspreader here?
      // TODO how can we deal with "collisions"?  Equal hashcodes?
      return address.hashCode() - address1.hashCode();
   }

}

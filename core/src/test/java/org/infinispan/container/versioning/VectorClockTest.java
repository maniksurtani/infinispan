/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
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
import org.infinispan.remoting.transport.Transport;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Test(testName = "container.versioning.VectorClockTest", groups = "unit")
public class VectorClockTest {
   public void testComparisons() {
      VectorClockGenerator node1 = createVectorClockGenerator();
      VectorClockGenerator node2 = createVectorClockGenerator();

      IncrementableEntryVersion clock = node1.generateNew();
      clock = node1.increment(clock);
      clock = node1.increment(clock);
      clock = node2.increment(clock);
      IncrementableEntryVersion clock1 = node2.increment(clock);
      IncrementableEntryVersion clock2 = node1.increment(clock1);

      assert clock2.compareTo(clock) == InequalVersionComparisonResult.AFTER;
      assert clock.compareTo(clock2) == InequalVersionComparisonResult.BEFORE;
      assert clock.compareTo(clock) == InequalVersionComparisonResult.EQUAL;
   }

   public void testConflicts() {
      VectorClockGenerator node1 = createVectorClockGenerator();
      VectorClockGenerator node2 = createVectorClockGenerator();

      IncrementableEntryVersion clock = node1.generateNew();
      clock = node1.increment(clock);
      clock = node1.increment(clock);
      clock = node2.increment(clock);

      // Now, node1 increments clock
      IncrementableEntryVersion clock1 = node1.increment(clock);

      // And so does node2
      IncrementableEntryVersion clock2 = node2.increment(clock);

      // Should have a conflict
      assert clock1.compareTo(clock2) == InequalVersionComparisonResult.CONFLICTING;
      assert clock2.compareTo(clock1) == InequalVersionComparisonResult.CONFLICTING;
   }

   public void testConflicts2() {
      VectorClockGenerator node1 = createVectorClockGenerator();
      VectorClockGenerator node2 = createVectorClockGenerator();

      IncrementableEntryVersion clock = node1.generateNew();
      clock = node1.increment(clock);
      clock = node1.increment(clock);

      // Now, node1 increments clock
      IncrementableEntryVersion clock1 = node1.increment(clock);

      // And so does node2
      IncrementableEntryVersion clock2 = node2.increment(clock);

      // Should have a conflict
      assert clock1.compareTo(clock2) == InequalVersionComparisonResult.CONFLICTING;
      assert clock2.compareTo(clock1) == InequalVersionComparisonResult.CONFLICTING;
   }


   private VectorClockGenerator createVectorClockGenerator() {
      VectorClockGenerator g = new VectorClockGenerator();
      Transport mockTransport = mock(Transport.class);
      Address mockAddress = mock(Address.class);
      when(mockTransport.getAddress()).thenReturn(mockAddress);
      g.init(mockTransport);
      return g;
   }
}

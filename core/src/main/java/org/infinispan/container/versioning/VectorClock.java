package org.infinispan.container.versioning;

import net.jcip.annotations.Immutable;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.FastCopyHashMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A vector clock implementation.
 *
 * @author Manik Surtani
 * @see VectorClockGenerator
 * @since 5.3
 */
@Immutable
public class VectorClock implements IncrementableEntryVersion, Cloneable {

   // Lets encode this as Map<Address, Integer> for now.
   private FastCopyHashMap<Address, Integer> clocks;

   private VectorClock(FastCopyHashMap<Address, Integer> clocks) {
      this.clocks = clocks;
   }

   public VectorClock(Address address) {
      clocks = new FastCopyHashMap<Address, Integer>(2);
      clocks.put(address, 1);
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other == null) throw new IllegalArgumentException("Cannot compare against a null value");
      if (!(other instanceof VectorClock))
         throw new IllegalArgumentException("Can only compare a vector clock with another vector clock; not with a " + other.getClass());
      VectorClock otherVC = (VectorClock) other;

      InequalVersionComparisonResult r = InequalVersionComparisonResult.EQUAL;

      for (Map.Entry<Address, Integer> e : otherVC.clocks.entrySet()) {
         Address a = e.getKey();
         int otherV = e.getValue();

         Integer myV = clocks.get(a);
         if (myV == null) {
            // other is ahead.
            if (r == InequalVersionComparisonResult.EQUAL) r = InequalVersionComparisonResult.BEFORE;
            else if (r == InequalVersionComparisonResult.AFTER) {
               r = InequalVersionComparisonResult.CONFLICTING;
               break; // break out of this loop.  Pointless checking for anything else.
            }
         } else {
            if (myV > otherV) {
               if (r == InequalVersionComparisonResult.EQUAL) r = InequalVersionComparisonResult.AFTER;
               else if (r == InequalVersionComparisonResult.BEFORE) {
                  r = InequalVersionComparisonResult.CONFLICTING;
                  break; // break out of this loop.  Pointless checking for anything else.
               }
            } else if (otherV > myV) {
               if (r == InequalVersionComparisonResult.EQUAL) r = InequalVersionComparisonResult.BEFORE;
               else if (r == InequalVersionComparisonResult.AFTER) {
                  r = InequalVersionComparisonResult.CONFLICTING;
                  break; // break out of this loop.  Pointless checking for anything else.
               }
            }
         }
      }

      // now, if we're not conflicting, look for addresses in current clock that are not in the 'other' clock.
      switch (r) {
         case EQUAL:
         case BEFORE:
            // if we have any addresses not in 'other', then we have a conflict.
            boolean ahead = false;
            for (Address a : clocks.keySet()) {
               ahead = ahead || !otherVC.clocks.containsKey(a);
            }

            if (ahead) {
               if (r == InequalVersionComparisonResult.BEFORE) r = InequalVersionComparisonResult.CONFLICTING;
               else r = InequalVersionComparisonResult.AFTER;
            }
            // don't care for other cases.
      }

      return r;
   }

   @Override
   public String toString() {
      return "VectorClock{" + clocks + '}';
   }

   @Override
   public VectorClock clone() {
      try {
         VectorClock newVC = (VectorClock) super.clone();
         newVC.clocks = clocks.clone();
         return newVC;
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }

   public VectorClock increment(Address address) {
      VectorClock incremented = this.clone();
      Integer currentClockValue = incremented.clocks.get(address);
      if (currentClockValue == null) currentClockValue = 0;
      incremented.clocks.put(address, ++currentClockValue);
      return incremented;
   }

   public static class Externalizer implements AdvancedExternalizer<VectorClock> {

      @Override
      public Set<Class<? extends VectorClock>> getTypeClasses() {
         return Collections.<Class<? extends VectorClock>>singleton(VectorClock.class);
      }

      @Override
      public void writeObject(ObjectOutput output, VectorClock object) throws IOException {
         output.writeObject(object.clocks);
      }

      @Override
      public VectorClock readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new VectorClock((FastCopyHashMap<Address, Integer>) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.PARTITION_AWARE_CLUSTERED_VERSION;
      }
   }
}

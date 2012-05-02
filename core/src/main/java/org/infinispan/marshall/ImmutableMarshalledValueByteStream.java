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

package org.infinispan.marshall;

import java.io.IOException;
import java.io.OutputStream;

/**
 * // TODO: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ImmutableMarshalledValueByteStream extends MarshalledValueOutputStream {
   private final byte[] bytes;

   public ImmutableMarshalledValueByteStream(byte[] bytes) {
      this.bytes = bytes;
   }

   @Override
   public int size() {
      return bytes.length;
   }

   @Override
   public byte[] getRaw() {
      return bytes;
   };

   @Override
   public void write(int b) throws IOException {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean equals(Object thatObject) {
      if (thatObject instanceof MarshalledValueOutputStream) {
         MarshalledValueOutputStream that = (MarshalledValueOutputStream) thatObject;
         if (this == that) return true;
         byte[] thoseBytes = that.getRaw();
         if (this.bytes == thoseBytes) return true;
         if (this.size() != that.size()) return false;
         for (int i=0; i<bytes.length; i++) {
            if (this.bytes[i] != thoseBytes[i]) return false;
         }
         return true;
      } else {
         return false;
      }
   }
}

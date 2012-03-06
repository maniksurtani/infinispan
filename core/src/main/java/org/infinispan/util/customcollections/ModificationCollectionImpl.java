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

import java.util.Arrays;
import java.util.Iterator;

/**
 * // TODO: MS: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ModificationCollectionImpl implements ModificationCollection {
   public static final ModificationCollection EMPTY_MODIFICATION_COLLECTION = new ModificationCollectionImpl();

   WriteCommand[] modifications;

   private ModificationCollectionImpl() {
   }

   public ModificationCollectionImpl(WriteCommand mod) {
      modifications = new WriteCommand[]{mod};
   }

   @Override
   public boolean contains(WriteCommand modification) {
      return CustomCollections.contains(modifications, modification);
   }

   @Override
   public boolean isEmpty() {
      return modifications.length == 0;
   }

   @Override
   public int size() {
      return modifications.length;
   }

   @Override
   public void add(WriteCommand modification) {
      modifications = CustomCollections.addToArray(modifications, modification);
   }

   @Override
   public Iterator<WriteCommand> iterator() {
      return new Iterator<WriteCommand>() {
         int idx = 0;
         @Override
         public boolean hasNext() {
            return idx < modifications.length;
         }

         @Override
         public WriteCommand next() {
            return modifications[idx++];
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException();
         }
      };
   }

   @Override
   public String toString() {
      return "WriteCommand{" +
            "" + (modifications == null ? null : Arrays.toString(modifications)) +
            '}';
   }

   @Override
   public KeyCollection getAffectedKeys() {
      if (modifications == null || modifications.length == 0) return KeyCollectionImpl.EMPTY_KEY_COLLECTION;
      if (modifications.length == 1) return modifications[0].getAffectedKeys();
      KeyCollection keys = new KeyCollectionImpl(modifications.length);
      for (WriteCommand wc: modifications) keys.addAll(wc.getAffectedKeys());
      return keys;
   }

   @Override
   public WriteCommand getFirst() {
      return modifications == null || modifications.length == 0 ? null : modifications[0];
   }

   @Override
   public ModificationCollection clone() {
      ModificationCollectionImpl dolly = new ModificationCollectionImpl();
      if (modifications != null) dolly.modifications = modifications.clone();
      return dolly;
   }

   public static ModificationCollection fromArray(WriteCommand... mods) {
      ModificationCollectionImpl mci = new ModificationCollectionImpl();
      mci.modifications = mods;
      return mci;
   }
}

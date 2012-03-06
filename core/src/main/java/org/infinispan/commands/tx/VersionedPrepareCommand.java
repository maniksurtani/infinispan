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

package org.infinispan.commands.tx;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.customcollections.ModificationCollection;

import java.util.Arrays;

/**
 * Same as {@link PrepareCommand} except that the transaction originator makes evident the versions of entries touched
 * and stored in a transaction context so that accurate write skew checks may be performed by the lock owner(s).
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedPrepareCommand extends PrepareCommand {
   public static final byte COMMAND_ID = 26;
   private EntryVersionsMap versionsSeen = null;

   public VersionedPrepareCommand() {
      super("");
   }

   public VersionedPrepareCommand(String cacheName, GlobalTransaction gtx, boolean onePhase, ModificationCollection modifications) {
      // VersionedPrepareCommands are *always* 2-phase, except when retrying a prepare.
      super(cacheName, gtx, onePhase, modifications);
   }

   public VersionedPrepareCommand(String cacheName) {
      super(cacheName);
   }

   public EntryVersionsMap getVersionsSeen() {
      return versionsSeen;
   }

   public void setVersionsSeen(EntryVersionsMap versionsSeen) {
      this.versionsSeen = versionsSeen;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, onePhaseCommit, versionsSeen, modifications};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      onePhaseCommit = (Boolean) args[1];
      versionsSeen = (EntryVersionsMap) args[2];
      modifications = (ModificationCollection) args[3];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "VersionedPrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", versionsSeen=" + versionsSeen +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}

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
package org.infinispan.commands.control;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.TransactionalInvocationContextFlagsOverride;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Set;


/**
 * LockControlCommand is a command that enables distributed locking across infinispan nodes.
 * <p/>
 * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class MultiKeyLockControlCommand extends LockControlCommand implements FlagAffectedCommand {

   public static final int COMMAND_ID = 29;
   private KeyCollection keys;

   private MultiKeyLockControlCommand() {
      super(null); // For command id uniqueness test
   }

   public MultiKeyLockControlCommand(String cacheName) {
      super(cacheName);
   }

   public MultiKeyLockControlCommand(String cacheName, Set<Flag> flags, GlobalTransaction gtx, KeyCollection keys) {
      super(cacheName);
      this.keys = keys;
      this.flags = flags;
      this.globalTx = gtx;
   }

   public KeyCollection getKeys() {
      return keys;
   }

   public void setKeys(KeyCollection newKeys) {
      this.keys = newKeys;
   }

   @Override
   public boolean multipleKeys() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, unlock, keys, flags};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unsupported command id:" + commandId);
      int i = 0;
      globalTx = (GlobalTransaction) args[i++];
      unlock = (Boolean) args[i++];
      keys = (KeyCollection) args[i++];
      flags = (Set<Flag>) args[i];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      MultiKeyLockControlCommand that = (MultiKeyLockControlCommand) o;

      if (unlock != that.unlock) return false;
      if (flags == null)
         return that.flags == null;
      else
      if (!flags.equals(that.flags)) return false;
      if (!Util.safeEquals(keys, that.keys)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + keys.hashCode();
      result = 31 * result + (unlock ? 1 : 0);
      if (flags != null)
         result = 31 * result + flags.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("SingleKeyLockControlCommand{cache=").append(cacheName)
            .append(", keys=").append(keys)
            .append(", flags=").append(flags)
            .append(", unlock=").append(unlock)
            .append("}")
            .toString();
   }
}

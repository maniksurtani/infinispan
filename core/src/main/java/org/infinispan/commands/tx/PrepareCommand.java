/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.commands.tx;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.customcollections.CustomCollections;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;
import org.infinispan.util.customcollections.ModificationCollection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;

/**
 * Command corresponding to the 1st phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand {

   private static final Log log = LogFactory.getLog(PrepareCommand.class);
   private boolean trace = log.isTraceEnabled();

   public static final byte COMMAND_ID = 12;

   protected ModificationCollection modifications;
   protected boolean onePhaseCommit;
   protected CacheNotifier notifier;
   protected RecoveryManager recoveryManager;
   private transient boolean replayEntryWrapping  = false;
   
   private static final WriteCommand[] EMPTY_WRITE_COMMAND_ARRAY = new WriteCommand[0];

   public void initialize(CacheNotifier notifier, RecoveryManager recoveryManager) {
      this.notifier = notifier;
      this.recoveryManager = recoveryManager;
   }

   private PrepareCommand() {
      super(null); // For command id uniqueness test
   }

   public PrepareCommand(String cacheName, GlobalTransaction gtx, boolean onePhaseCommit, ModificationCollection modifications) {
      super(cacheName);
      this.globalTx = gtx;
      this.modifications = modifications;
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(String cacheName) {
      super(cacheName);
   }

   public Object perform(InvocationContext ignored) throws Throwable {
      if (ignored != null)
         throw new IllegalStateException("Expected null context!");

      if (recoveryManager != null && recoveryManager.isTransactionPrepared(globalTx)) {
         log.tracef("The transaction %s is already prepared. Skipping prepare call.", globalTx);
         return null;
      }

      // 1. first create a remote transaction
      RemoteTransaction remoteTransaction = txTable.getRemoteTransaction(globalTx);
      boolean remoteTxInitiated = remoteTransaction != null;
      if (!remoteTxInitiated) {
         remoteTransaction = txTable.createRemoteTransaction(globalTx, modifications);
      } else {
         /*
          * remote tx was already created by Cache#lock() API call
          * set the proper modifications since lock has none
          * 
          * @see LockControlCommand.java 
          * https://jira.jboss.org/jira/browse/ISPN-48
          */
         remoteTransaction.setModifications(getModifications());
      }

      // 2. then set it on the invocation context
      RemoteTxInvocationContext ctx = icc.createRemoteTxInvocationContext(remoteTransaction, getOrigin());

      if (trace)
         log.tracef("Invoking remotely originated prepare: %s with invocation context: %s", this, ctx);
      notifier.notifyTransactionRegistered(ctx.getGlobalTransaction(), ctx);
      return invoker.invoke(ctx, this);
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareCommand((TxInvocationContext) ctx, this);
   }

   public ModificationCollection getModifications() {
      return CustomCollections.nullCheck(modifications);
   }

   public boolean isOnePhaseCommit() {
      return onePhaseCommit;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, onePhaseCommit, modifications};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      onePhaseCommit = (Boolean) args[1];
      modifications = (ModificationCollection) args[2];
   }

   public PrepareCommand copy() {
      PrepareCommand copy = new PrepareCommand(cacheName);
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      return copy;
   }

   @Override
   public String toString() {
      return "PrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", " + super.toString();
   }

   public boolean containsModificationType(Class<? extends ReplicableCommand> replicableCommandClass) {
      for (WriteCommand mod : getModifications()) {
         if (mod.getClass().equals(replicableCommandClass)) {
            return true;
         }
      }
      return false;
   }

   public boolean hasModifications() {
      return modifications != null && !modifications.isEmpty();
   }

   public KeyCollection getAffectedKeys() {
      if (modifications == null || modifications.isEmpty()) return KeyCollectionImpl.EMPTY_KEY_COLLECTION;
      return modifications.getAffectedKeys();
   }

   /**
    * If set to true, then the keys touched by this transaction are to be wrapped again and original ones discarded.
    */
   public boolean isReplayEntryWrapping() {
      return replayEntryWrapping;
   }

   /**
    * @see #isReplayEntryWrapping()
    */
   public void setReplayEntryWrapping(boolean replayEntryWrapping) {
      this.replayEntryWrapping = replayEntryWrapping;
   }

   public boolean writesToASingleKey() {
      if (modifications == null || modifications.size() != 1)
         return false;
      WriteCommand wc = modifications.getFirst();
      return wc instanceof PutKeyValueCommand || wc instanceof RemoveCommand || wc instanceof ReplaceCommand ||
            (wc instanceof PutMapCommand && ((PutMapCommand) wc).getMap().size() == 1);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }
}

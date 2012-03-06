/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.interceptors.locking;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.CacheException;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.TimSort;
import org.infinispan.util.customcollections.KeyCollection;
import org.infinispan.util.customcollections.KeyCollectionImpl;
import org.infinispan.util.customcollections.ModificationCollection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   final LockAcquisitionVisitor lockAcquisitionVisitor = new LockAcquisitionVisitor();
   private static final MurmurHash3 HASH = new MurmurHash3();
   private final static Comparator<Object> keyComparator = new Comparator<Object>() {

      @Override
      public int compare(Object o1, Object o2) {
         int thisVal = HASH.hash(o1);
         int anotherVal = HASH.hash(o2);
         return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
      }
   };

   EntryFactory entryFactory;

   private static final Log log = LogFactory.getLog(OptimisticLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void setDependencies(EntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      abortIfRemoteTransactionInvalid(ctx, command);
      if (!command.hasModifications() || command.writesToASingleKey()) {
         //optimisation: don't create another LockReorderingVisitor here as it is not needed.
         log.trace("Not using lock reordering as we have a single key.");
         acquireLocksVisitingCommands(ctx, command);
      } else {
         KeyCollection orderedKeys = sort(command.getModifications());
         boolean hasClear = orderedKeys == null;
         if (hasClear) {
            log.trace("Not using lock reordering as the prepare contains a clear command.");
            acquireLocksVisitingCommands(ctx, command);
         } else {
            if (log.isTraceEnabled()) log.tracef("Using lock reordering, order is: %s", orderedKeys);
            acquireAllLocks(ctx, orderedKeys);
            ctx.addAllAffectedKeys(orderedKeys);
         }
      }
      return invokeNextAndCommitIf1Pc(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }
   
   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         for (Object key : dataContainer.keySet())
            entryFactory.wrapEntryForClear(ctx, key);
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new CacheException("Explicit locking is not allowed with optimistic caches!");
   }

   private final class LockAcquisitionVisitor extends AbstractVisitor {

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : dataContainer.keySet()) {
            lockAndRegisterBackupLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         for (Object key : command.getMap().keySet()) {
            lockAndRegisterBackupLock(txC, key);
            txC.addAffectedKey(key);
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }
      
      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cdl.localNodeIsOwner(command.getKey())) {
            KeyCollection compositeKeys = command.getCompositeKeys();
            for (Object key : compositeKeys) {
               lockAndRegisterBackupLock((TxInvocationContext) ctx, key);
            }            
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         final TxInvocationContext txC = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txC, command.getKey());
         txC.addAffectedKey(command.getKey());
         return null;
      }
   }
   
   private KeyCollection sort(ModificationCollection writes) {
      KeyCollection kc = new KeyCollectionImpl(writes.size(), true);
      for (WriteCommand wc: writes) {
         switch (wc.getCommandId()) {
            case ClearCommand.COMMAND_ID:
               return null;
            case PutKeyValueCommand.COMMAND_ID:
            case RemoveCommand.COMMAND_ID:
            case ReplaceCommand.COMMAND_ID:
               kc.add(((DataWriteCommand) wc).getKey());
               break;
            case PutMapCommand.COMMAND_ID:
               kc.addAll(wc.getAffectedKeys());
               break;
            case ApplyDeltaCommand.COMMAND_ID:
               ApplyDeltaCommand command = (ApplyDeltaCommand) wc;
               if (cdl.localNodeIsOwner(command.getKey())) {
                  KeyCollection compositeKeys = command.getCompositeKeys();
                  kc.addAll(compositeKeys);
               }
               break;
         }
      }
      kc.sort(keyComparator);
      return kc;
   }

   private void acquireAllLocks(TxInvocationContext ctx, KeyCollection orderedKeys) throws InterruptedException {
      for (Object key: orderedKeys) lockAndRegisterBackupLock(ctx, key);
   }

   private void acquireLocksVisitingCommands(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      for (WriteCommand wc : command.getModifications()) {
         wc.acceptVisitor(ctx, lockAcquisitionVisitor);
      }
   }
}

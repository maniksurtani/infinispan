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

package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.NetworkPartitionException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;

/**
 * An interceptor that handles network partitions when they are detected.
 *
 * @author Manik Surtani
 * @since 5.3
 */
public class PartitionHandlingInterceptor extends CommandInterceptor {
   private volatile boolean readOnly= false;
   private Cache<?, ?> cache;
   private TransactionTable transactionTable;

   // TODO this should be configurable
   private static final int MIN_NODES_FOR_PRIMARY_PARTITION = 3;

   @Inject
   public void setComponents(Cache<?, ?> cache, TransactionTable transactionTable) {
      this.cache = cache;
      this.transactionTable = transactionTable;
   }


   @Start
   public void start() {
      if (cacheConfiguration.clustering().cacheMode().isClustered()) {
         // Register a listener
         PartitionListener l = new PartitionListener();
         cache.getCacheManager().addListener(l);

      }

   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (readOnly) {
         throw new NetworkPartitionException();
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   @Listener
   public class PartitionListener {
      @ViewChanged
      public void onViewChange(ViewChangedEvent vce) {
         if (!vce.isMergeView() && vce.getNewMembers().size() < vce.getOldMembers().size()) {
            // Could be a partition.
            // check primary partition threshold.
            if (vce.getNewMembers().size() < MIN_NODES_FOR_PRIMARY_PARTITION) {
               PartitionHandlingInterceptor.this.readOnly = true;
               if (cacheConfiguration.transaction().transactionMode().isTransactional()) {
                  for (LocalTransaction tx: transactionTable.getLocalTransactions()) tx.markForRollback(true);
                  for (RemoteTransaction tx: transactionTable.getRemoteTransactions()) tx.markForRollback(true);
               }
            }
         } else {
            if (vce.isMergeView()) {
               // TODO: check that a state transfer happens, and if so,
               PartitionHandlingInterceptor.this.readOnly = false;
            }
         }
      }
   }
}

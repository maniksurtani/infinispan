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

import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Transport;

/**
 * A factory for creating new {@link VectorClock} instances.
 *
 * @see VectorClock
 * @author Manik Surtani
 * @since 5.3
 */
public class VectorClockGenerator implements VersionGenerator {

   private Transport transport;

   @Inject
   public void init(Transport transport) {
      this.transport = transport;
   }

   @Start(priority = 11)
   public void start() {
      if (transport == null) throw new ConfigurationException("Using vector clock based versioning in a non-clustered cache is not supported");
   }

   @Override
   public IncrementableEntryVersion generateNew() {
      return new VectorClock(transport.getAddress());
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      return ((VectorClock) initialVersion).increment(transport.getAddress());
   }
}

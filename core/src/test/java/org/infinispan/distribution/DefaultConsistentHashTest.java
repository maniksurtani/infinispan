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
package org.infinispan.distribution;

import org.infinispan.config.Configuration;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.AddressCollection;
import org.infinispan.util.AddressCollectionFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "distribution.DefaultConsistentHashTest")
public class DefaultConsistentHashTest extends AbstractInfinispanTest {

   public DefaultConsistentHash createConsistentHash(AddressCollection servers) {
      Configuration c = new Configuration().fluent()
            .hash().consistentHashClass(DefaultConsistentHash.class)
            .build();
      return (DefaultConsistentHash) ConsistentHashHelper.createConsistentHash(c, servers);
   }

   public void testSimpleHashing() {
      AddressCollection servers = AddressCollectionFactory.allAddresses(new TestAddress(1), new TestAddress(2), new TestAddress(3), new TestAddress(4));
      DefaultConsistentHash ch = createConsistentHash(servers);

      Object o = new Object();
      AddressCollection l1 = ch.locate(o, 2);
      AddressCollection l2 = ch.locate(o, 2);

      assert l1.size() == 2;
      assert l1.equals(l2);
      assert l1 != l2;

      Object o2 = new Object() {
         @Override
         public int hashCode() {
            return 4567890;
         }
      };

      Object o3 = new Object() {
         @Override
         public int hashCode() {
            return 4567890;
         }
      };

      assert o2 != o3;
      assert !o2.equals(o3);
      assert ch.locate(o2, 4).equals(ch.locate(o3, 4));
   }

   public void testMultipleKeys() {
      AddressCollection servers = AddressCollectionFactory.allAddresses(new TestAddress(1), new TestAddress(2), new TestAddress(3), new TestAddress(4));
      DefaultConsistentHash ch = createConsistentHash(servers);

      Object k1 = "key1", k2 = "key2", k3 = "key3";
      Collection<Object> keys = Arrays.asList(k1, k2, k3);
      Map<Object, AddressCollection> locations = ch.locateAll(keys, 3);

      assert locations.size() == 3;
      for (Object k : keys) {
         assert locations.containsKey(k);
         assert locations.get(k).size() == 3;
      }
   }

   public void testNumHashedNodes() {
      AddressCollection servers = AddressCollectionFactory.allAddresses(new TestAddress(1), new TestAddress(2), new TestAddress(3), new TestAddress(4));
      DefaultConsistentHash ch = createConsistentHash(servers);

      String[] keys = new String[10000];
      Random r = new Random();
      for (int i=0; i<10000; i++) keys[i] = Integer.toHexString(r.nextInt());

      for (String key: keys) {
         AddressCollection l = ch.locate(key, 2);
         assert l.size() == 2: "Did NOT find 2 owners for key ["+key+"] as expected!  Found " + l;
      }
   }

   public void testEveryNumOwners() {
      for (int nodesCount = 1; nodesCount < 10; nodesCount++) {
         AddressCollection servers = AddressCollectionFactory.emptyCollection();
         for (int i = 0; i < nodesCount; i++) {
            servers.add(new TestAddress(i * 1000));
         }

         DefaultConsistentHash ch = createConsistentHash(servers);
         AddressCollection sortedServers = ch.getCaches().clone();

         // check that we get numOwners servers for numOwners in 1..nodesCount
         for (int numOwners = 1; numOwners < nodesCount; numOwners++) {
            for (int i = 0; i < nodesCount; i++) {
               AddressCollection owners = ch.locate(sortedServers.get(i), numOwners);
               assertEquals(owners.size(), numOwners);
               for (int j = 0; j < numOwners; j++) {
                  assertEquals(owners.get(j), sortedServers.get((i + j) % nodesCount));
               }
            }
         }

         // check that we get all the servers for numOwners > nodesCount
         for (int i = 0; i < nodesCount; i++) {
            AddressCollection owners = ch.locate(sortedServers.get(i), nodesCount + 1);
            assertEquals(owners.size(), nodesCount);
            for (int j = 0; j < nodesCount; j++) {
               assertEquals(owners.get(j), sortedServers.get((i + j) % nodesCount));
            }
         }
      }
   }
}


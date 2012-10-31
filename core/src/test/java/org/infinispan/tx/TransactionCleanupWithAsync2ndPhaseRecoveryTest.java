package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "TransactionReleaseWithAsync2ndPhaseRecoveryTest")
public class TransactionCleanupWithAsync2ndPhaseRecoveryTest extends TransactionCleanupWithAsync2ndPhaseTest {

   @Override
   protected ConfigurationBuilder getConfiguration() {
      final ConfigurationBuilder dcc = super.getConfiguration();
      dcc.transaction().recovery();
      return dcc;
   }

}

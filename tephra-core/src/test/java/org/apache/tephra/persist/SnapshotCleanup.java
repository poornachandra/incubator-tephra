/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.persist;

import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.snapshot.SnapshotCodecV4;
import org.junit.Test;

import java.io.File;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 */
public class SnapshotCleanup {

  @Test
  public void cleanup() throws Exception {
    File testDir = new File("/Users/poorna/tickets/1424");
    Configuration conf = new Configuration();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV4.class.getName());

    LocalFileTransactionStateStorage storage =
      new LocalFileTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
    storage.startAndWait();
    TransactionSnapshot snapshot = storage.getLatestSnapshot();
    System.out.println("Snapshot time = " + snapshot.getTimestamp());
    System.out.println("Invalid tx size = " + snapshot.getInvalid().size());
    System.out.println("In-progress size = " + snapshot.getInProgress().size());
    System.out.println("Old snapshot = " + snapshot);

    long truncateWp = 1510605708602L * TxConstants.MAX_TX_PER_MS;

    TreeSet<Long> invalid = new TreeSet<>(snapshot.getInvalid());
    SortedSet<Long> newInvalid = invalid.tailSet(truncateWp);
    System.out.println("Removing tx ids " + invalid.headSet(truncateWp).size() + " from invalid list");

    System.out.println("New invalid size = " + newInvalid.size());

    TransactionSnapshot snapshot1 =
      new TransactionSnapshot(snapshot.getTimestamp() + 1,
                              snapshot.getReadPointer(), snapshot.getWritePointer(),
                              newInvalid,
                              snapshot.getInProgress(), snapshot.getCommittingChangeSets(),
                              snapshot.getCommittedChangeSets());
    System.out.println("New snapshot = " + snapshot1);

    storage.writeSnapshot(snapshot1);
    storage.stopAndWait();
  }

  @Test
  public void verify() throws Exception {
    File testDir = new File("/Users/poorna/tickets/1424");
    Configuration conf = new Configuration();
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV4.class.getName());

    LocalFileTransactionStateStorage storage =
      new LocalFileTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
    storage.startAndWait();
    TransactionSnapshot snapshot = storage.getLatestSnapshot();
    System.out.println("Latest snapshot after cleanup = " + snapshot);

    System.out.println("Invalid list - " + snapshot.getInvalid());
  }
}

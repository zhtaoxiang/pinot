/**
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
package org.apache.pinot.segment.local.dedup;

import java.util.List;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;

class ConcurrentMapPartitionDedupMetadataManager implements PartitionDedupMetadataManager {
  private final PartitionDedupMetadataManager _partitionDedupMetadataManagerDelegate;

  public ConcurrentMapPartitionDedupMetadataManager(String tableNameWithType, List<String> primaryKeyColumns,
      int partitionId, ServerMetrics serverMetrics, HashFunction hashFunction, double metadataTTL,
      String metadataTimeColumn) {
    if (metadataTTL > 0) {
      _partitionDedupMetadataManagerDelegate =
          new RetentionConcurrentMapPartitionDedupMetadataManager(tableNameWithType, primaryKeyColumns, partitionId,
              serverMetrics, hashFunction, metadataTTL, metadataTimeColumn);
    } else {
      _partitionDedupMetadataManagerDelegate =
          new NoRetentionConcurrentMapPartitionDedupMetadataManager(tableNameWithType, primaryKeyColumns, partitionId,
              serverMetrics, hashFunction);
    }
  }

  @Override
  public void addSegment(IndexSegment segment) {
    _partitionDedupMetadataManagerDelegate.addSegment(segment);
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    _partitionDedupMetadataManagerDelegate.removeSegment(segment);
  }

  @Override
  public void removeExpiredPrimaryKeys() {
    _partitionDedupMetadataManagerDelegate.removeExpiredPrimaryKeys();
  }

  @Override
  public boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment) {
    return _partitionDedupMetadataManagerDelegate.checkRecordPresentOrUpdate(pk, indexSegment);
  }

  @Override
  public boolean dropOrAddRecord(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    return _partitionDedupMetadataManagerDelegate.dropOrAddRecord(dedupRecordInfo, indexSegment);
  }
}

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
package org.apache.pinot.common.minion;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Base abstract class for task generator info.
 *
 * This info gets serialized and stored in zookeeper under the path:
 * MINION_TASK_GENERATOR_INFO/${tableNameWithType}/${taskName} if it's associated with a table, or
 * MINION_TASK_GENERATOR_INFO/${taskName} if it's not associated with a table.
 */
public abstract class BaseTaskGeneratorInfo {
  /**
   * @return task type
   */
  public abstract String getTaskType();

  /**
   * @return {@link ZNRecord} containing the task generator info
   */
  public abstract ZNRecord toZNRecord();

  /**
   * @return task generator info as a Json string
   */
  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}

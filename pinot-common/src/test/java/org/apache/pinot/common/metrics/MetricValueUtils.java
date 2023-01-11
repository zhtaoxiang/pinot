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
package org.apache.pinot.common.metrics;

import com.yammer.metrics.core.MetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerSettableGauge;
import org.apache.pinot.spi.metrics.PinotMetric;


public class MetricValueUtils {
  private MetricValueUtils() {
  }

  public static boolean gaugeExists(AbstractMetrics metrics, String metricName) {
    return extractMetric(metrics, metricName) != null;
  }

  public static long getGaugeValue(AbstractMetrics metrics, String metricName) {
    PinotMetric pinotMetric = extractMetric(metrics, metricName);
    if (pinotMetric == null) {
      return 0;
    }
    return ((YammerSettableGauge<Long>) pinotMetric.getMetric()).value();
  }

  private static PinotMetric extractMetric(AbstractMetrics metrics, String metricName) {
    String metricPrefix;
    Class metricClass;
    if (metrics instanceof ControllerMetrics) {
      metricPrefix = "pinot.controller.";
      metricClass = ControllerMetrics.class;
    } else if (metrics instanceof BrokerMetrics) {
      metricPrefix = "pinot.broker.";
      metricClass = BrokerMetrics.class;
    } else if (metrics instanceof ServerMetrics) {
      metricPrefix = "pinot.server.";
      metricClass = ServerMetrics.class;
    } else if (metrics instanceof MinionMetrics) {
      metricPrefix = "pinot.minion.";
      metricClass = MinionMetrics.class;
    } else {
      throw new RuntimeException("unsupported AbstractMetrics type");
    }
    return metrics.getMetricsRegistry().allMetrics()
        .get(new YammerMetricName(new MetricName(metricClass, metricPrefix + metricName)));
  }
}

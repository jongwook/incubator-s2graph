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

package org.hbase.async

import net.bytebuddy.implementation.bind.annotation.This
import org.hbase.async.generated.HBasePB.TimeRange
import org.hbase.async.generated.{FilterPB, ClientPB}
import org.jboss.netty.buffer.ChannelBuffer

object GetRequestExtra {

  trait IsGetRequest {
    def isGetRequest: Boolean
  }

  trait StoreLimitOffset {
    def getStoreLimit: Int
    def getStoreOffset: Int
    def setStoreLimit(storeLimit: Int): Unit
    def setStoreOffset(storeOffset: Int): Unit
  }

  type GetRequestExtra = org.hbase.async.GetRequest with IsGetRequest with StoreLimitOffset

  def serialize(server_version: Byte, @This request: GetRequestExtra): ChannelBuffer = {
    val getpb = ClientPB.Get.newBuilder.setRow(Bytes.wrap(request.key()))

    if (request.family != null) {
      val column = ClientPB.Column.newBuilder
      column.setFamily(Bytes.wrap(request.family))
      if (request.qualifiers != null) {
        for (qualifier <- request.qualifiers) {
          column.addQualifier(Bytes.wrap(qualifier))
        }
      }
      getpb.addColumn(column.build)
    }

    // Filters
    val filter = request.getFilter
    if (filter != null) {
      getpb.setFilter(FilterPB.Filter.newBuilder.setNameBytes(Bytes.wrap(filter.name)).setSerializedFilter(Bytes.wrap(filter.serialize)).build)
    }

    // TimeRange
    val min_ts = request.getMinTimestamp
    val max_ts = request.getMaxTimestamp
    if (min_ts != 0 || max_ts != Long.MaxValue) {
      val time = TimeRange.newBuilder
      if (min_ts != 0) {
        time.setFrom(min_ts)
      }
      if (max_ts != Long.MaxValue) {
        time.setTo(max_ts)
      }
      getpb.setTimeRange(time.build)
    }

    if (request.maxVersions != 1) {
      getpb.setMaxVersions(request.maxVersions)
    }
    if (!request.isGetRequest) {
      getpb.setExistenceOnly(true)
    }

    // storeOffset and storeLimit
    if (request.getStoreLimit > 0) {
      getpb.setStoreLimit(request.getStoreLimit)
    }
    if (request.getStoreOffset > 0) {
      getpb.setStoreOffset(request.getStoreOffset)
    }

    val get = ClientPB.GetRequest.newBuilder.setRegion(request.region.toProtobuf).setGet(getpb.build)

    HBaseRpc.toChannelBuffer(GetRequest.GGET, get.build)
  }

}

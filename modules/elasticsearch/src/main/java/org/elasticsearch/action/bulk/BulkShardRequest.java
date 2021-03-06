/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BulkShardRequest extends ShardReplicationOperationRequest {

    private int shardId;

    private BulkItemRequest[] items;

    BulkShardRequest() {
    }

    BulkShardRequest(String index, int shardId, BulkItemRequest[] items) {
        this.index = index;
        this.shardId = shardId;
        this.items = items;
    }

    int shardId() {
        return shardId;
    }

    BulkItemRequest[] items() {
        return items;
    }

    /**
     * Before we fork on a local thread, make sure we copy over the bytes if they are unsafe
     */
    @Override public void beforeLocalFork() {
        for (BulkItemRequest item : items) {
            ((ShardReplicationOperationRequest) item.request()).beforeLocalFork();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
        out.writeVInt(items.length);
        for (BulkItemRequest item : items) {
            item.writeTo(out);
        }
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
        items = new BulkItemRequest[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            items[i] = BulkItemRequest.readBulkItem(in);
        }
    }
}

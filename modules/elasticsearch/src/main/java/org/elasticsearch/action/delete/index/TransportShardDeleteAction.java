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

package org.elasticsearch.action.delete.index;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportShardDeleteAction extends TransportShardReplicationOperationAction<ShardDeleteRequest, ShardDeleteResponse> {

    @Inject public TransportShardDeleteAction(Settings settings, TransportService transportService,
                                              ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                              ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override protected boolean checkWriteConsistency() {
        return true;
    }

    @Override protected ShardDeleteRequest newRequestInstance() {
        return new ShardDeleteRequest();
    }

    @Override protected ShardDeleteResponse newResponseInstance() {
        return new ShardDeleteResponse();
    }

    @Override protected String transportAction() {
        return "indices/index/b_shard/delete";
    }

    @Override protected void checkBlock(ShardDeleteRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override protected ShardDeleteResponse shardOperationOnPrimary(ClusterState clusterState, ShardOperationRequest shardRequest) {
        ShardDeleteRequest request = shardRequest.request;
        IndexShard indexShard = indexShard(shardRequest);
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id());
        delete.refresh(request.refresh());
        indexShard.delete(delete);
        return new ShardDeleteResponse();
    }

    @Override protected void shardOperationOnReplica(ShardOperationRequest shardRequest) {
        ShardDeleteRequest request = shardRequest.request;
        IndexShard indexShard = indexShard(shardRequest);
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id());
        delete.refresh(request.refresh());
        indexShard.delete(delete);
    }

    @Override protected ShardsIterator shards(ClusterState clusterState, ShardDeleteRequest request) {
        GroupShardsIterator group = clusterService.operationRouting().broadcastDeleteShards(clusterService.state(), request.index());
        for (ShardsIterator shards : group) {
            if (shards.shardId().id() == request.shardId()) {
                return shards;
            }
        }
        throw new ElasticSearchIllegalStateException("No shards iterator found for shard [" + request.shardId() + "]");
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.Randomness;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexModule;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Utility class for {@link LocalShardsBalancer and @link RemoteShardsBalancer}.
 * @opensearch.internal
 */
@ExperimentalApi
public class ShardsBalancerUtils {

    /**
     * Returns a shuffled queue of {@link RoutingNode} from the given routing nodes.
     * @param routingNodes routing nodes to be shuffled.
     * @return shuffled queue of routing nodes.
     */
    public static Queue<RoutingNode> getShuffledRemoteNodes(RoutingNodes routingNodes) {
        List<RoutingNode> nodeList = getRemoteRoutingNodes(routingNodes);
        Randomness.shuffle(nodeList);
        return new ArrayDeque<>(nodeList);
    }

    /**
     * Filters out and returns the list of {@link RoutingPool#REMOTE_CAPABLE} nodes from the routing nodes in cluster.
     * @param routingNodes routing nodes to be shuffled.
     * @return list of {@link RoutingPool#REMOTE_CAPABLE} routing nodes.
     */
    public static List<RoutingNode> getRemoteRoutingNodes(RoutingNodes routingNodes) {
        List<RoutingNode> nodeList = new ArrayList<>();
        for (RoutingNode rNode : routingNodes) {
            if (RoutingPool.REMOTE_CAPABLE.equals(RoutingPool.getNodePool(rNode))) {
                nodeList.add(rNode);
            }
        }
        return nodeList;
    }

    /**
     * Filters out indices in the given routing allocation with the given tiering state (can be HOT_TO_WARM/WARM_TO_HOT)
     * @param allocation routing allocation
     * @param tieringState tiering state to filter
     * @return Set of indices that are in the given state of tiering
     */
    public static Set<String> getIndicesPendingTiering(RoutingAllocation allocation, String tieringState) {
        Set<String> filteredIndices = new HashSet<>();
        allocation.routingTable().indicesRouting().keySet().stream().forEach(index -> {
            if (tieringState.equals(allocation.metadata().index(index).getSettings().get(IndexModule.INDEX_TIERING_STATE.getKey()))) {
                filteredIndices.add(index);
            }
        });
        return filteredIndices;
    }

    /**
     * Finds out all the shards of an index that are pending shard relocation
     * @param index index name
     * @return the list of shards that are pending relocation
     */
    public static List<ShardRouting> getShardsPendingRelocation(final String index, RoutingAllocation allocation) {
        final List<ShardRouting> shardsPendingRelocation = new ArrayList<>();
        final List<ShardRouting> indexShards = allocation.routingTable().index(index).shardsWithState(ShardRoutingState.STARTED);
        for (ShardRouting shard : indexShards) {
            RoutingPool targetPool = RoutingPool.getShardPool(shard, allocation);
            RoutingPool currentNodePool = RoutingPool.getNodePool(allocation.routingNodes().node(shard.currentNodeId()));
            if (RoutingPool.REMOTE_CAPABLE.equals(targetPool) && targetPool != currentNodePool) {
                shardsPendingRelocation.add(shard);
            }
        }
        return shardsPendingRelocation;
    }
}

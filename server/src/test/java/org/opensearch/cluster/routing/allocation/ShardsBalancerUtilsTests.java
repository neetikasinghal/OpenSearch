/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexModule;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.opensearch.cluster.routing.allocation.allocator.ShardsBalancerUtils.getIndicesPendingTiering;
import static org.opensearch.cluster.routing.allocation.allocator.ShardsBalancerUtils.getRemoteRoutingNodes;
import static org.opensearch.cluster.routing.allocation.allocator.ShardsBalancerUtils.getShardsPendingRelocation;
import static org.opensearch.cluster.routing.allocation.allocator.ShardsBalancerUtils.getShuffledRemoteNodes;

public class ShardsBalancerUtilsTests extends TieringAllocationBaseTestCase {

    private RoutingNodes routingNodes;
    private RoutingAllocation routingAllocation;
    private ClusterState clusterState;
    private AllocationService service;
    protected static final int REMOTE_NODES = 3;
    protected static final int LOCAL_NODES = 5;
    protected static final int LOCAL_INDICES = 5;
    protected static final int REMOTE_INDICES = 0;

    @Before
    void beforeTest() {
        clusterState = createInitialCluster(LOCAL_NODES, REMOTE_NODES, LOCAL_INDICES, REMOTE_INDICES);
        service = this.createRemoteCapableAllocationService();
        // assign shards to respective nodes
        clusterState = allocateShardsAndBalance(clusterState, service);
        routingNodes = clusterState.getRoutingNodes();
        routingAllocation = getRoutingAllocation(clusterState, routingNodes);
    }

    public void testGetShuffledRemoteNodes() {
        List<RoutingNode> remoteRoutingNodes = getRemoteRoutingNodes(routingNodes);
        List<RoutingNode> shuffledRemoteNodes = new ArrayList<>(getShuffledRemoteNodes(routingNodes));
        assertEquals(remoteRoutingNodes.size(), shuffledRemoteNodes.size());
    }

    public void testGetRemoteRoutingNodes() {
        List<RoutingNode> remoteRoutingNodes = getRemoteRoutingNodes(routingNodes);
        assertEquals(REMOTE_NODES, remoteRoutingNodes.size());
        for (RoutingNode rNode : remoteRoutingNodes) {
            assertEquals(RoutingPool.REMOTE_CAPABLE, RoutingPool.getNodePool(rNode));
        }
    }

    public void testGetIndicesPendingTiering() {
        Set<String> indicesPendingTiering = getIndicesPendingTiering(routingAllocation, IndexModule.TieringState.HOT_TO_WARM.name());
        assertTrue(indicesPendingTiering.isEmpty());
        // put indices in the hot to warm tiering state
        RoutingAllocation updatedRoutingAllocation = getRoutingAllocationForHotToWarmTiering();
        Set<String> tieringSet = getIndicesPendingTiering(updatedRoutingAllocation, IndexModule.TieringState.HOT_TO_WARM.name());
        assertEquals(LOCAL_INDICES, tieringSet.size());
    }

    public void testGetShardsPendingRelocation() {
        RoutingAllocation updatedRoutingAllocation = getRoutingAllocationForHotToWarmTiering();
        for (int i = 0; i < LOCAL_INDICES; i++) {
            List<ShardRouting> shardsPendingRelocation = getShardsPendingRelocation(getIndexName(i, false), updatedRoutingAllocation);
            assertFalse(shardsPendingRelocation.isEmpty());
        }
    }

    private RoutingAllocation getRoutingAllocationForHotToWarmTiering() {
        ClusterState updatedClusterState = updateIndexMetadataForTiering(
            clusterState,
            LOCAL_INDICES,
            IndexModule.TieringState.HOT_TO_WARM.name(),
            IndexModule.DataLocalityType.PARTIAL.name()
        );
        return getRoutingAllocation(updatedClusterState, updatedClusterState.getRoutingNodes());
    }
}

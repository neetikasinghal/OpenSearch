/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

//@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class CardinalityWithRequestBreakerIT extends OpenSearchIntegTestCase {

//    public CardinalityWithRequestBreakerIT(Settings dynamicSettings) {
//        super(dynamicSettings);
//    }
//
//    @ParametersFactory
//    public static Collection<Object[]> parameters() {
//        return Arrays.asList(
//            new Object[][]{new Object[]{Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build()}}
////            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
//        );
//    }

//    @Override
//    protected Settings featureFlagSettings() {
//        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
//    }

    /**
     * Test that searches using cardinality aggregations returns all request breaker memory.
     */
    public void testRequestBreaker() throws Exception {
        final String requestBreaker = "528" + "kb";
        logger.info("--> Using request breaker setting: {}", requestBreaker);

//        System.out.println("The value of docs is " + randomIntBetween(10, 1000));
        indexRandom(
            true,
            IntStream.range(0, 190)
                .mapToObj(
                    i -> client().prepareIndex("test")
                        .setId("id_" + i)
                        .setSource(Map.of("field0", randomAlphaOfLength(5), "field1", randomAlphaOfLength(5)))
                )
                .toArray(IndexRequestBuilder[]::new)
        );

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestBreaker)
            )
            .get();

//        indexRandomForConcurrentSearch("test");
//        System.out.println("The random vars for search are " + randomFrom(Aggregator.SubAggCollectionMode.values()) + " " + BucketOrder.aggregation("cardinality", randomBoolean())
//        + " " + randomLongBetween(1, 40000));
        try {
            SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
                .addAggregation(
                    terms("terms").field("field0.keyword")
                        .collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST)
                        .order(BucketOrder.aggregation("cardinality", false))
                        .subAggregation(cardinality("cardinality").precisionThreshold(31541).field("field1.keyword"))
                );
            System.out.println("The search request is " + searchRequestBuilder);
            searchRequestBuilder.get();
        } catch (OpenSearchException e) {
            if (ExceptionsHelper.unwrap(e, CircuitBreakingException.class) == null) {
                throw e;
            }
        }

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()))
            .get();

//        Thread.sleep(5000);
        // validation done by InternalTestCluster.ensureEstimatedStats()
    }
}

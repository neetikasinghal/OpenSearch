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

package org.opensearch.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class BucketsAggregatorTests extends AggregatorTestCase {

    public BucketsAggregator buildMergeAggregator() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("numeric", 0));
                indexWriter.addDocument(document);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);

                SearchContext searchContext = createSearchContext(
                    indexSearcher,
                    createIndexSettings(),
                    null,
                    new MultiBucketConsumerService.MultiBucketConsumer(
                        DEFAULT_MAX_BUCKETS,
                        new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                    ),
                    new NumberFieldMapper.NumberFieldType("test", NumberFieldMapper.NumberType.INTEGER)
                );

                return new BucketsAggregator("test", AggregatorFactories.EMPTY, searchContext, null, null, null) {
                    @Override
                    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
                        return null;
                    }

                    @Override
                    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
                        return new InternalAggregation[0];
                    }

                    @Override
                    public InternalAggregation buildEmptyAggregation() {
                        return null;
                    }
                };
            }
        }
    }

    public void testBucketMergeNoDelete() throws IOException {
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        for (int i = 0; i < 10; i++) {
            mergeAggregator.incrementBucketDocCount(i, i);
        }

        mergeAggregator.mergeBuckets(10, bucket -> bucket % 5);

        for (int i = 0; i < 5; i++) {
            // The i'th bucket should now have all docs whose index % 5 = i
            // This is buckets i and i + 5
            // i + (i+5) = 2*i + 5
            assertEquals(mergeAggregator.getDocCounts().get(i), (2 * i) + 5);
        }
        for (int i = 5; i < 10; i++) {
            assertEquals(mergeAggregator.getDocCounts().get(i), 0);
        }
    }

    public void testBucketMergeAndDelete() throws IOException {
        BucketsAggregator mergeAggregator = buildMergeAggregator();

        mergeAggregator.grow(10);
        int sum = 0;
        for (int i = 0; i < 20; i++) {
            mergeAggregator.incrementBucketDocCount(i, i);
            if (5 <= i && i < 15) {
                sum += i;
            }
        }

        // Put the buckets in indices 5 ... 14 into bucket 5, and delete the rest of the buckets
        mergeAggregator.mergeBuckets(10, bucket -> (5 <= bucket && bucket < 15) ? 5 : -1);

        assertEquals(mergeAggregator.getDocCounts().size(), 10); // Confirm that the 10 other buckets were deleted
        for (int i = 0; i < 10; i++) {
            assertEquals(mergeAggregator.getDocCounts().get(i), i == 5 ? sum : 0);
        }
    }

    @Repeat(iterations = 1000, useConstantSeed = true)
    public void testMultiConsumerAcceptOne() throws Exception {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker
        );

        when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        int numberOfThreads = 5;
        ExecutorService service = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            service.submit(() -> {
                try {
                    multiBucketConsumer.accept1(0);
                } catch (CircuitBreakingException e) {
                    System.out.println("CircuitBreaker was thrown");
                }
                latch.countDown();
            });
        }
        long end = System.currentTimeMillis();
        latch.await();
        System.out.println("DEBUG: accept1 took " + (end - start) + " MilliSeconds");
    }

    @Repeat(iterations = 1000, useConstantSeed = true)
    public void testMultiConsumerAcceptFunctionTwo() throws InterruptedException {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker
        );

        when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        int numberOfThreads = 5;
        ExecutorService service = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            service.submit(() -> {
                try {
                    multiBucketConsumer.accept2(0);
                } catch (CircuitBreakingException e) {
                    System.out.println("CircuitBreaker was thrown");
                }
                latch.countDown();
            });
        }
        long end = System.currentTimeMillis();
        latch.await();
        System.out.println("DEBUG: accept2 took " + (end - start) + " MilliSeconds");
    }

    @Repeat(iterations = 1000, useConstantSeed = true)
    public void testMultiConsumerAcceptFunctionThree() throws InterruptedException {
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker
        );

        when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        int numberOfThreads = 5;
        ExecutorService service = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfThreads; i++) {
            service.submit(() -> {
                try {
                    multiBucketConsumer.accept3(0);
                } catch (CircuitBreakingException e) {
                    System.out.println("CircuitBreaker was thrown");
                }
                latch.countDown();
            });
        }
        long end = System.currentTimeMillis();
        latch.await();
        System.out.println("DEBUG: accept3 took " + (end - start) + " MilliSeconds");
    }
}

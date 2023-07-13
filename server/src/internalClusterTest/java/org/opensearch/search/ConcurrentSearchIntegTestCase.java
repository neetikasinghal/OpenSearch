/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runners.Parameterized;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

public class ConcurrentSearchIntegTestCase extends OpenSearchIntegTestCase {
    protected final boolean featureFlag;

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {true},
            new Object[] {false}
        );
    }
    public ConcurrentSearchIntegTestCase(boolean featureFlag) {
        this.featureFlag = featureFlag;
    }

    @Before
    void beforeTests() {
        ClusterUpdateSettingsResponse clusterUpdateSettingsResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), featureFlag)).get();
        assertEquals(String.valueOf(featureFlag), clusterUpdateSettingsResponse.getPersistentSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()));
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        for (Setting builtInFlag : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            featureSettings.put(builtInFlag.getKey(), builtInFlag.getDefaultRaw(Settings.EMPTY));
        }
        featureSettings.put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, true);
        return featureSettings.build();
    }
}

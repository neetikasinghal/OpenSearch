# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Fix for hasInitiatedFetching to fix allocation explain and manual reroute APIs (([#14972](https://github.com/opensearch-project/OpenSearch/pull/14972))
- [Workload Management] Add queryGroupId to Task ([14708](https://github.com/opensearch-project/OpenSearch/pull/14708))
- Add setting to ignore throttling nodes for allocation of unassigned primaries in remote restore ([#14991](https://github.com/opensearch-project/OpenSearch/pull/14991))
- [Streaming Indexing] Enhance RestClient with a new streaming API support ([#14437](https://github.com/opensearch-project/OpenSearch/pull/14437))
- Add basic aggregation support for derived fields ([#14618](https://github.com/opensearch-project/OpenSearch/pull/14618))
- Add ThreadContextPermission for markAsSystemContext and allow core to perform the method ([#15016](https://github.com/opensearch-project/OpenSearch/pull/15016))
- Add ThreadContextPermission for stashAndMergeHeaders and stashWithOrigin ([#15039](https://github.com/opensearch-project/OpenSearch/pull/15039))
- [Concurrent Segment Search] Support composite aggregations with scripting ([#15072](https://github.com/opensearch-project/OpenSearch/pull/15072))
- Add `rangeQuery` and `regexpQuery` for `constant_keyword` field type ([#14711](https://github.com/opensearch-project/OpenSearch/pull/14711))
- Add took time to request nodes stats ([#15054](https://github.com/opensearch-project/OpenSearch/pull/15054))
- [Workload Management] QueryGroup resource tracking framework changes ([#13897](https://github.com/opensearch-project/OpenSearch/pull/13897))
- HotToWarmTieringService changes to tier shards ([#14891](https://github.com/opensearch-project/OpenSearch/pull/14891))

### Dependencies
- Bump `netty` from 4.1.111.Final to 4.1.112.Final ([#15081](https://github.com/opensearch-project/OpenSearch/pull/15081))
- Bump `org.apache.commons:commons-lang3` from 3.14.0 to 3.15.0 ([#14861](https://github.com/opensearch-project/OpenSearch/pull/14861))
- OpenJDK Update (July 2024 Patch releases) ([#14998](https://github.com/opensearch-project/OpenSearch/pull/14998))
- Bump `com.microsoft.azure:msal4j` from 1.16.1 to 1.16.2 ([#14995](https://github.com/opensearch-project/OpenSearch/pull/14995))
- Bump `actions/github-script` from 6 to 7 ([#14997](https://github.com/opensearch-project/OpenSearch/pull/14997))
- Bump `org.tukaani:xz` from 1.9 to 1.10 ([#15110](https://github.com/opensearch-project/OpenSearch/pull/15110))
- Bump `actions/setup-java` from 1 to 4 ([#15104](https://github.com/opensearch-project/OpenSearch/pull/15104))
- Bump `org.apache.avro:avro` from 1.11.3 to 1.12.0 in /plugins/repository-hdfs ([#15119](https://github.com/opensearch-project/OpenSearch/pull/15119))
- Bump `org.bouncycastle:bcpg-fips` from 1.0.7.1 to 2.0.8 and `org.bouncycastle:bc-fips` from 1.0.2.5 to 2.0.0 in /distribution/tools/plugin-cli ([#15103](https://github.com/opensearch-project/OpenSearch/pull/15103))
- Bump `com.azure:azure-core` from 1.49.1 to 1.51.0 ([#15111](https://github.com/opensearch-project/OpenSearch/pull/15111))

### Changed
- Add lower limit for primary and replica batch allocators timeout ([#14979](https://github.com/opensearch-project/OpenSearch/pull/14979))

### Deprecated

### Removed

### Fixed
- Fix constraint bug which allows more primary shards than average primary shards per index ([#14908](https://github.com/opensearch-project/OpenSearch/pull/14908))
- Fix NPE when bulk ingest with empty pipeline ([#15033](https://github.com/opensearch-project/OpenSearch/pull/15033))
- Fix missing value of FieldSort for unsigned_long ([#14963](https://github.com/opensearch-project/OpenSearch/pull/14963))
- Fix delete index template failed when the index template matches a data stream but is unused ([#15080](https://github.com/opensearch-project/OpenSearch/pull/15080))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x

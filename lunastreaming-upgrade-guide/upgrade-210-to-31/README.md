# Lunastreaming Upgrade Guide

## Overview

Upgrading Lunastreaming from version 2.10 to 3.1 is crucial for organizations seeking to leverage the latest features and improvements in messaging and streaming data. This upgrade introduces significant enhancements in performance, security, and functionality that can greatly benefit our deployment.

This document covers the impact of the upgrade, including new features that enhance scalability and reliability, as well as breaking changes that may affect our current setup. It is intended for system administrators, DevOps engineers, and developers who are familiar with Lunastreaming and its deployment options.

We will provide a comprehensive overview of the upgrade process, including detailed instructions for bare metal, Docker, and Kubernetes deployments, along with important documentation on breaking changes, the upgrade sequence, and specific upgrade steps. Adhering to these procedures is essential for a successful transition, minimizing downtime and ensuring that our messaging infrastructure remains robust and efficient.

## Upgrade Impact

As we prepare to upgrade Lunastreaming from version 2.10 to 3.1, the following sections will guide us through the process, ensuring a smooth transition and minimizing potential issues.

### Functional Changes

#### Default System Topics

System topics are enabled by default, facilitating better management and monitoring of subscriptions and tenants.

#### Prometheus Metrics Changes
- Prometheus Client version has changed from `0.5.0` to `0.16.0`.
- The metric type `UNTYPED` is renamed to `UNKNOWN`.
- Metrics have been renamed to include a `_total` suffix as per OpenMetrics standards.

##### Renamed Metrics

| Before                              | After                               |
|-------------------------------------|-------------------------------------|
| pulsar_expired_token_count          | pulsar_expired_token_total          |
| pulsar_authentication_success_count  | pulsar_authentication_success_total  |
| pulsar_authentication_failures_count | pulsar_authentication_failures_total |
| pulsar_source_received_total_1min   | pulsar_source_received_1min_total   |
| pulsar_source_written_total_1min    | pulsar_source_written_1min_total    |
| pulsar_source_source_total_1min     | pulsar_source_source_total_1min     |
| pulsar_source_source_exceptions_1min_total | pulsar_source_source_exceptions_1min_total |
| pulsar_source_system_exceptions_total_1min | pulsar_source_system_exceptions_1min_total |
| pulsar_function_received_total_1min | pulsar_function_received_1min_total |
| pulsar_function_user_exceptions_total_1min | pulsar_function_user_exceptions_1min_total |
| pulsar_function_system_exceptions_total_1min | pulsar_function_system_exceptions_1min_total |
| pulsar_function_processed_successfully_total_1min | pulsar_function_processed_successfully_1min_total |
| pulsar_sink_received_total_1min     | pulsar_sink_received_1min_total     |
| pulsar_sink_written_total_1min      | pulsar_sink_written_1min_total      |
| pulsar_sink_sink_exceptions_total_1min | pulsar_sink_sink_exceptions_1min_total |
| pulsar_sink_system_exceptions_total_1min | pulsar_sink_system_exceptions_1min_total |
| pulsar_lb_unload_broker_count       | pulsar_lb_unload_broker_total       |
| pulsar_lb_unload_bundle_count        | pulsar_lb_unload_bundle_total        |
| pulsar_lb_bundles_split_count        | pulsar_lb_bundles_split_total        |
| pulsar_schema_del_ops_failed_count   | pulsar_schema_del_ops_failed_total   |
| pulsar_schema_get_ops_failed_count   | pulsar_schema_get_ops_failed_total   |
| pulsar_schema_put_ops_failed_count   | pulsar_schema_put_ops_failed_total   |
| pulsar_schema_compatible_count        | pulsar_schema_compatible_total       |
| pulsar_schema_incompatible_count      | pulsar_schema_incompatible_total     |
| pulsar_txn_committed_count           | pulsar_txn_committed_total          |
| pulsar_txn_aborted_count             | pulsar_txn_aborted_total            |
| pulsar_txn_created_count             | pulsar_txn_created_total            |
| pulsar_txn_timeout_count             | pulsar_txn_timeout_total            |
| pulsar_txn_append_log_count          | pulsar_txn_append_log_total         |


###### Related Pull Requests

- [Bump prometheus client version from 0.5.0 to 0.15.0](https://github.com/apache/pulsar/pull/13785)
- [Rename Pulsar txn metrics to specify OpenMetrics](https://github.com/apache/pulsar/pull/16581)
- [Rename Pulsar schema metrics to specify OpenMetrics](https://github.com/apache/pulsar/pull/16610)

#### Other Functional Impacts

| PR Link                                 | Title                                                                                                   | Functional Impact                                                                                                 |
|-----------------------------------------|---------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| [PR #19180](https://github.com/apache/pulsar/pull/19180) | [cleanup][broker] Deprecate blocking AuthorizationService, AuthorizationProvider methods               | This will affect the public API for the AuthorizationService and the AuthorizationProvider, impacting users with custom code inside the Pulsar Broker. |
| [PR #19182](https://github.com/apache/pulsar/pull/19182) | [cleanup][broker] Remove AuthorizationProvider methods deprecated in 2.7 and 2.9                       | Removes deprecated methods such as `allowTenantOperationAsync`, `allowNamespaceOperationAsync`, etc. These methods could be used by third-party extensions. |
| [PR #19197](https://github.com/apache/pulsar/pull/19197) | [feat][broker] Update AuthenticationProvider to simplify HTTP Authn                                    | Changes the public API within the broker as some methods are marked as @Deprecated.                              |
| [PR #19295](https://github.com/apache/pulsar/pull/19295) | [feat][broker] OneStageAuth State: move authn out of constructor                                       | This may break third-party plugins if they rely on authentication happening in the constructor, with a fail-fast behavior introduced. |
| [PR #19314](https://github.com/apache/pulsar/pull/19314) | [fix][broker] TokenAuthenticationState: authenticate token only once                                   | This breaks an implicit contract, but misuse will likely lead to a fail-fast behavior with exceptions thrown.     |
| [PR #19455](https://github.com/apache/pulsar/pull/19455) | [improve][broker] Require authRole is proxyRole to set originalPrincipal                               | Affects binary protocol usage; upgrading existing proxies requires correct configuration in `broker.conf`.        |
| [PR #19486](https://github.com/apache/pulsar/pull/19486) | [improve][client] Remove default 30s ackTimeout when setting DLQ policy on Java consumer              | Removes the default `ackTimeoutMillis` when a dead letter policy is set; must be specified exclusively.          |

### Configuration Changes

#### Dropped Configurations in 3.1
- **broker.conf / standalone.conf**

| PR Link                                 | Config with Default Value                     | Description                                                     |
|-----------------------------------------|-----------------------------------------------|-----------------------------------------------------------------|
| [PR #14506](https://github.com/apache/pulsar/pull/14506) | managedLedgerNumWorkerThreads=                | Number of threads to be used for managed ledger tasks dispatching |


#### Deprecated & Default Value Changes
- **broker.conf / standalone.conf**

| Config                                    | 2.10                      | 3.1                      |
|-------------------------------------------|---------------------------|--------------------------|
| managedLedgerCacheEvictionFrequency       | `100.0`                   | Deprecated               |
| managedLedgerMaxUnackedRangesToPersistInZooKeeper | `1000`         | Deprecated: use `managedLedgerMaxUnackedRangesToPersistInMetadataStore` |

#### Default value changed in 3.1
- **broker.conf / standalone.conf**

| Parameter                                         | 2.10 Value                                                 | 3.1 Value                                                                                       |
|---------------------------------------------------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| Enable or disable system topic                    | systemTopicEnabled=false                                   | systemTopicEnabled=true                                                                          |
| Enable or disable topic level policies             | topicLevelPoliciesEnabled=false                            | topicLevelPoliciesEnabled=true                                                                   |
| Supported algorithms for namespace bundle split    | supportedNamespaceBundleSplitAlgorithms=range_equally_divide,topic_count_equally_divide,specified_positions_divide | supportedNamespaceBundleSplitAlgorithms=range_equally_divide,topic_count_equally_divide,specified_positions_divide,flow_or_qps_equally_divide |
| Load balancer direct memory resource weight       | loadBalancerDirectMemoryResourceWeight=1.0                | loadBalancerDirectMemoryResourceWeight=0                                                        |
| File system profile path                          | fileSystemProfilePath=../conf/filesystem_offload_core_site.xml | fileSystemProfilePath=conf/filesystem_offload_core_site.xml                                    |
| GCS ledger offload max block size in bytes       | gcsManagedLedgerOffloadMaxBlockSizeInBytes=67108864      | gcsManagedLedgerOffloadMaxBlockSizeInBytes=134217728                                           |


### Operational Changes

#### Upgrade to JDK 17

Lunastreaming 3.1 uses JDK 17 for the server modules.

#### Removed Python 2 Support

Python 2 has been removed from build scripts; Python 3 is now used.

### Known Issues

#### Bookkeeper / RocksDB Format

Issues may arise when downgrading from Lunastreaming 3.1 back to 2.10 due to incompatibility between RocksDB versions.

**Stack Trace for Downgrade Failure**:
```
2024-02-23T11:42:14,155+0000 [main] ERROR org.apache.bookkeeper.server.Main - Failed to build bookie server java.io.IOException: Error open RocksDB database
```

**Conclusion**: Pulsar 3.1 uses RocksDB 7.x, which writes in a format incompatible with RocksDB 6.x used by Lunastreaming 2.10.

For more details, refer to [Bug #22051](https://github.com/apache/pulsar/issues/22051).


## Upgrade Procedure

Lunastreaming can be deployed using three primary modes: Bare Metal, Docker, and Kubernetes. Each mode has its advantages and considerations, allowing organizations to choose the best fit for their infrastructure and operational needs.

This section provides a sequence of steps to upgrade Pulsar.

For detailed deployment instructions specific to each environment (Docker-Compose, Kubernetes, and Bare Metal), please refer to the following separate README files:

- **Bare Metal Deployment**: For detailed instructions on deploying and upgrading Lunastreaming on bare metal servers, check the [Bare Metal README](bare-metal.md).

- **Docker-Compose Deployment**: For information on upgrading and configuring Lunastreaming in a Docker environment, please see the [Docker README](docker-compose.md).

- **Kubernetes using Helm Chart Deployment**: For guidance on upgrading and managing Lunastreaming in a Kubernetes cluster using pulsar helm chart, refer to the [Kubernetes README](kubernetes-using-helm-chart.md).

- **Kubernetes with KAAP Deployment**: For guidance on upgrading and managing Lunastreaming in a Kubernetes cluster using KAAP, refer to the [Kubernetes README](kubernetes-with-kaap.md).


Each README file provides environment-specific steps, configurations, and considerations to ensure a smooth upgrade process. Be sure to follow the instructions relevant to your deployment type.

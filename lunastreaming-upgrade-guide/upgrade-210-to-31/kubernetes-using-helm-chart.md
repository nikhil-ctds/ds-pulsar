<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

This guide is for migrating pulsar cluster deployed on kubernetes using
[pulsar-helm-chart](https://github.com/datastax/pulsar-helm-chart).

## Steps for migration

### Prepare the Environment

- Ensure you have access to a running Kubernetes cluster via a workstation where `kubectl` is installed.
- Ensure `helm` is installed and configured to work with your Kubernetes cluster.
- Ensure the current version of Pulsar running in your cluster is `2.10`.
- Make sure you clone the [datastax/pulsar-helm-chart](https://github.com/datastax/pulsar-helm-chart) repo.

### Backup Existing Configuration

Before proceeding with the upgrade, back up your existing Pulsar configurations to prevent data loss.

Save current Helm release configurations using.

```bash
helm get values <release-name> > pulsar-backup-values.yaml
```

### Update Helm Repository

Update the DataStax Pulsar Helm chart repository.
```bash
helm repo update
```

### Modify Configuration for Version Upgrade

Open `helm-chart-sources/pulsar/values.yaml` and update the following fields.

- Update the image.tag to `3.1_0.1` (or the specific tag you wish to use).
- To modify other configurations, update the values.yaml as needed in the
  config section.

For Example
```yaml
image:
  broker:
    # If not using tiered storage, you can use the smaller pulsar image for the broker
    repository: datastax/lunastreaming-all
    pullPolicy: IfNotPresent
    tag: 3.1_0.1
    component: broker
    replicaCount: 2
    configData:
      brokerDeduplicationEnabled: "false"
  brokerSts:
    # If not using tiered storage, you can use the smaller pulsar image for the broker
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latest
  function:
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latest
  zookeeper:
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latestupgr
  bookkeeper:
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latest
  proxy:
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latest
  bastion:
    repository: apachepulsar/pulsar
    pullPolicy: IfNotPresent
    tag: latest
```

### Install or Upgrade Pulsar

Use Helm to upgrade your existing Pulsar installation. The `--wait` flag ensures that Helm waits until all pods are ready
before completing the upgrade.

```bash
helm upgrade  --namespace pulsar --wait --debug --timeout 1200s  --dependency-update pulsar <pulsar-helm-chart-repo-dir>/helm-chart-sources/pulsar --values current-values.yaml
```

### Monitor Upgrade Process

Check the status of the pods to ensure they are running correctly.
```bash
kubectl get pods --namespace pulsar
```
If there are any issues, check the logs.
```bash
kubectl logs <pod-name> -n <namespace>
```

### Post-Upgrade Configuration Changes

- Ensure that all necessary configurations are in place and correct after the upgrade.
- After upgrading, check if any additional configurations are required for new features in version 3.1.
- Adjust settings related to multi-tenancy, security, and observability as needed.


### Validate Functionality

- Test the functionality of your Pulsar cluster by sending messages and ensuring that consumers can read them without
  issues.
- Conduct functional tests to ensure that the upgrade did not impact existing applications and that new features work
  as expected.
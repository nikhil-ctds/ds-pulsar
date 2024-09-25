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

This guide is for migrating lunastreaming cluster deployed on Docker compose.

## Steps for migration

### Update the Docker Compose File

- Identify the `docker-compose.yml` file by navigating to where your lunastreaming services are defined.
- Update the lunastreaming image version for all components to the desired version (for example, `datastax/lunastreaming:3.1_4.5`). 
- Ensure that any dependent services referencing the lunastreaming image are updated accordingly. 
  - For example:
      ```
      services:
        zookeeper:
          image: datastax/lunastreaming:3.1_4.5
      …………
        pulsar-init:
          image: datastax/lunastreaming:3.1_4.5
      …………
        bookie:
          image: datastax/lunastreaming:3.1_4.5
      …………
        broker:
          image: datastax/lunastreaming:3.1_4.5
    ```
- Review and apply configuration changes as necessary by verifying and adjusting any environment variables or configurations that may have changed between versions.

### Backup Existing Configuration

Before proceeding with the upgrade, back up your existing lunastreaming configurations to prevent data loss.

### Pull the New Image

The following command to retrieve the most recent images specified in your docker-compose.yml file:
```
docker-compose pull
```
This command fetches the updated images, ensuring that you have the latest versions of all services defined in the configuration.

### Pull the New Image

Stop and Remove Existing Containers to ensure a clean upgrade by executing the following command:
```
docker-compose down
```
Note that this process will result in downtime for your lunastreaming services.

### Start the updated deployment

Start the Updated Deployment by executing the following command:
```
docker-compose up -d
```
This command will create new containers based on the updated images and configurations, ensuring the lunastreaming services are up and running.

### Verify the upgrade deployment
After starting the containers, use the following command to ensure that lunastreaming has started correctly without any errors:
```
docker-compose logs -f lunastreaming
```
- Confirm that you can access the Pulsar Admin UI at `http://localhost:8080` (or the configured port) to verify that all components are functioning as expected.
- Use Pulsar’s built-in health check commands or APIs to ensure all services are operational.

### Additional Considerations

- Compatibility: Review the release notes for any breaking changes between versions 2.10 and 3.1. Make any necessary adjustments to your application code to ensure compatibility.
- Testing: If feasible, conduct tests of your upgraded setup in a staging environment prior to deploying it to production. This will help identify any potential issues before affecting your live environment.

# Upgrading Pulsar on Bare-Metal

## Overview

This guide provides essential instructions for upgrading a Lunastreaming cluster. It covers upgrade sequences for different components, including ZooKeeper, bookies, brokers, and proxies. Proper planning and execution are crucial to minimize risks and ensure a smooth upgrade process.

### Components Overview

Apache Pulsar consists of the following components:
- **ZooKeeper**: Generally does not require upgrading unless specific needs arise.
- **Bookies**: Stateful components that must be upgraded first.
- **Brokers**: Stateless components that follow bookies in the upgrade sequence.
- **Proxies**: Also stateless and should be upgraded after brokers.


For detailed instructions on upgrading Lunastreaming on bare-metal environments, please refer to the official documentation. It provides comprehensive guidelines tailored specifically for bare-metal deployments, ensuring a smooth upgrade process.

[Official Apache Pulsar Upgrade Documentation](https://pulsar.apache.org/docs/next/administration-upgrade/)


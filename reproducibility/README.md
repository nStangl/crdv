# Reproducibility

## Requirements

- Linux[^1] (Recommended) / MacOS / Windows **instance** with internet connection
  - Minimum recommended specs: 8 vCPUs, 16 GB RAM, 100 GB NVMe SSD
- **Docker**
  - To install in Windows: https://docs.docker.com/desktop/install/windows-install/
  - To install in MacOS: https://docs.docker.com/desktop/install/mac-install/
  - To install in Linux: https://docs.docker.com/engine/install/
    - The following command can also be used in some Linux distributions (Ubuntu/Debian/RHEL/Fedora, with RHEL/Fedora requiring an additional `sudo service docker start`):
    ```shell
    curl -fsSL https://get.docker.com | sh
    ```

[^1]: Successfully tested with the following Linux distributions:
  Ubuntu (24.04)
  Debian (12.6)
  RHEL (9.4)
  Fedora (40)
  SUSE (15.6)
  Amazon Linux (2023)
  Arch

## Run

Assumes `reproducibility` as the current working directory. 
`sudo` must be provided in the Unix systems if `docker` needs to run as root.
To run smaller/faster tests, see the [Configuration](#configuration) section.

### Run everything

*Estimated runtime: **25 hours** with the default configuration or **8 hours** with the lite configuration.*

- Linux/MacOS/Windows WSL
```shell
chmod +x run_unix.sh
./run_unix.sh
```

- Windows (Powershell)
```shell
./run_windows.ps1
```

This will run all tests in the paper. The raw results will be available at `results`. A PDF document compiling all plots is also provided at `document/main.pdf`.

### Run specific tests

To run a specific test, we can pass it as an argument to the respective run script:

```shell
# unix
./run_unix.sh [test1 test2 ...] [cleanup]

# windows
./run_windows.ps1 [test1 test2 ...] [cleanup]
```

Example:
```shell
# runs the "timestamp_encoding" and "nested" tests;
# also deletes the main container and image
./run_unix.sh timestamp_encoding nested cleanup
```

The script creates the main image and container to execute the tests, if they do not exist.
The `cleanup` flag deletes the main container and image, so it should be used when we do not need to execute any more tests.

List of available tests:

| Test name | Label |
|---|---|
| `timestamp_encoding` | Figure 6 |
| `materialization_strategy` | Figure 7 |
| `plan_optimization` | Listing 3 |
| `nested_structures` | Figure 8 |
| `operations` | Figure 9 |
| `concurrency` | Figure 10 |
| `storage_structures` | Figure 11 |
| `storage_sites` | Figure 12 |
| `network` | Figure 13 |
| `freshness` | Figure 14 |
| `multiple_sites` | Figure 15 |


## Configuration

The default configuration uses the same parameters as the paper. To use different parameters, the respective configuration files located at `conf/` can be updated.

### Lite tests

To obtain results quicker, we provide versions of the configurations with reduced execution times, number of repeated runs, and X-axis values. Although faster, we expect the conclusions to remain the same.

To use the smaller configurations:

```shell
# backup the default configuration
cp -r conf conf_default
# set the new configuration
cp conf_lite/* conf
```

To restore the default configuration:
```shell
cp conf_default/* conf
```


## Differences from the paper

- For ease of use, these reproducibility scripts use Docker to deploy multiple sites. In the paper, however, this was accomplished using native deployments on multiple AWS virtual machines, each with its dedicated resources (CPU, Mem, Disk). This means that the order of magnitude of the results might be different, especially for the `multiple_sites` test, where each site is limited to 1 vCPU - to evaluate the scalability - and all sites share the same disk. However, we still expect the results to match the paper's conclusions.
- Since the `freshness` tests with Pg_crdt can consume large amounts of memory, they might stop early when running with many clients. To prevent this, the system should have swap enabled. However, it is not critical for the paper's conclusions.

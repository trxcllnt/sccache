# Distributed sccache

Background:

 - You should read about JSON Web Tokens - https://jwt.io/.
   - HS256 in short: you can sign a piece of (typically unencrypted)
     data with a key. Verification involves signing the data again
     with the same key and comparing the result. As a result, if you
     want two parties to verify each others messages, the key must be
     shared beforehand.
 - Secure token's referenced below should be generated with a CSPRNG
   (your OS random number generator should suffice).
   For example, on Linux this is accessible with: `openssl rand -hex 64`.
 - When relying on random number generators (for generating keys or
   tokens), be aware that a lack of entropy is possible in cloud or
   virtualized environments in some scenarios.

## Overview

Distributed sccache consists of five parts:

 - The client, an sccache binary that wishes to perform a compilation on
   remote machines.
 - The Scheduler(s) (`sccache-dist` binary), responsible for accepting and
   storing toolchains, job inputs, and enqueueing build jobs to be run.
 - The Server(s) (`sccache-dist` binary), responsible for actually executing
   build jobs.
 - The message broker, responsible for enqueueing and distributing jobs
   to available servers.
 - Storage for toolchains and job inputs and build results.
   Can be S3, Redis, Memcached, or any other storage type sccache supports.

   **Note**: While disk storage is supported, the disk must be shared between
   all schedulers and servers, i.e. scheduler and server are running locally,
   or using a shared NFS volume. It is strongly encouraged to use one of the
   other alternatives.

All servers are required to be an x86/ARM 64-bit Linux or a FreeBSD install.
Clients may request compilation from Linux, Windows or macOS.
Linux compilations will attempt to automatically package the compiler in use,
while Windows and macOS users will need to specify a toolchain for cross-
compilation ahead of time.

## Message Brokers

The sccache-dist scheduler and servers communicate via an external message
broker, either an AMQP v0.9.1 implementation (like RabbitMQ) or Redis.

The message broker is a third-party service, and is responsible for reliable
message delivery, acknowledgement, retries, and reporting failures.

All major CSPs provide managed AMQP or Redis services, or you can deploy
RabbitMQ or Redis as part of your infrastructure.

### Managed Message Broker Services

Here is a (non-exhaustive) list of managed message broker services available
in popular cloud service providers.

RabbitMQ:
* [AWS AMQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/welcome.html)
* [GCP RabbitMQ Connector](https://cloud.google.com/integration-connectors/docs/connectors/rabbitmq/configure)
* [CloudAMQP](https://www.cloudamqp.com/) and [CloudAMQP on GCP](https://www.cloudamqp.com/docs/google-GCP-marketplace-rabbitmq.html)

Valkey/Redis:
* [AWS ElastiCache for Redis](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WhatIs.html)
* [GCP Memorystore](https://cloud.google.com/memorystore/?hl=en)
* [Azure Managed Redis](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/managed-redis/managed-redis-overview)

### Running locally

For local development or custom deployments, you can run the message broker locally.

#### RabbitMQ
To run `sccache-dist` locally with RabbitMQ as the message broker, either:
1. Run the `rabbitmq` docker container:
  ```shell
  docker run --rm --name sccache-dist-rabbitmq -p 5672:5672 rabbitmq:latest
  ```
2. Install and run the RabbitMQ service (instructions [here](https://www.rabbitmq.com/docs/platforms))

Then configure `sccache-dist` to use your `rabbitmq` instance via either:
* Setting the `AMQP_ADDR=amqp://127.0.0.1:5672//` environment variable
* Adding `message_broker.amqp = "amqp://127.0.0.1:5672//"` to your scheduler config file

*Note:* The two slashes at the end of `amqp` address above is not a typo.

#### Redis

To run `sccache-dist` locally with Redis as the message broker, either:
1. Run the `redis` docker container:
  ```shell
  docker run --rm --name sccache-dist-redis -p 6379:6379 redis:latest
  ```
2. Install and run the RabbitMQ service (instructions [here](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/))

Then configure `sccache-dist` to use your `rabbitmq` instance via either:
* Setting the `REDIS_ADDR=redis://127.0.0.1:6379/` environment variable
* Adding `message_broker.redis = "redis://127.0.0.1:6379"` to your scheduler config file


## Scheduler and server storage
You can configure the `sccache-dist` scheduler and server storage (for any
supported storage type) with the same cache envvars as in `Configuration.md`,
but replace the `SCCACHE_` prefix with `SCCACHE_DIST_JOBS_` and
`SCCACHE_DIST_TOOLCHAINS_` respectively.

For example, to use Redis for job inputs/result storage, and S3 for toolchains:
```shell
SCCACHE_DIST_JOBS_REDIS_ENDPOINT=redis://127.0.0.1:6379/
SCCACHE_DIST_JOBS_REDIS_USERNAME=redis-user
SCCACHE_DIST_JOBS_REDIS_PASSWORD=redis-pass
SCCACHE_DIST_JOBS_REDIS_TTL=3600
SCCACHE_DIST_TOOLCHAINS_BUCKET=my-toolchains
SCCACHE_DIST_TOOLCHAINS_REGION=us-east-2
SCCACHE_DIST_TOOLCHAINS_S3_USE_SSL=true
```

This is the equivalent `scheduler.conf`/`server.conf` configuration:
```toml
# Job inputs/result storage. Must match server configuration.
# Can be any supported storage type (see Configuration.md)
[jobs.redis]
endpoint = "redis://127.0.0.1:6379/"
username = "redis-user"
password = "redis-pass"
ttl = 3600

# Toolchains storage. Must match server configuration.
# Can be any supported storage type (see Configuration.md)
[toolchains.s3]
bucket = "my-toolchains"
region = "us-east-2"
use_ssl = true
```


## Communication

The HTTP implementation of sccache has the following API, where all HTTP body content is encoded using [`bincode`](http://docs.rs/bincode):

 - scheduler
   - `GET /api/v2/status`
      - Returns information about the scheduler and servers.
   - `HEAD /api/v2/toolchain/:archive_id`
      - Called by the client to check if a toolchain exists.
   - `DELETE /api/v2/toolchain/:archive_id`
      - Called by the client to delete a toolchain.
   - `PUT /api/v2/toolchain/:archive_id`
      - Called by the client to (re)submit a toolchain.
   - `POST /api/v2/jobs/new`
      - Called by a client to create a new compile job for a certain input and
        toolchain.
      - Returns a new job ID, whether the toolchain exists, and a server-
        configured timeout after which the client should consider the job
        abandoned.
   - `PUT /api/v2/job/:job_id`
      - Called by a client to re-submit the inputs for an abandoned job.
   - `POST /api/v2/job/:job_id`
      - Called by a client to run a job.
   - `DELETE /api/v2/job/:job_id`
      - Called by a client to delete job inputs and result after receiving
        the job result.
   - `GET /metrics` (optional if a Prometheus path is configured)
      - Called by the metrics collector to scrape scheduler metrics.
      - Endpoint path is configurable via `scheduler.conf`
 - `server`
   - `GET <addr>/` (optional if a Prometheus addr is configured)
      - Called by the metrics collector to scrape server metrics.
      - Endpoint address is configurable via `server.conf`

There are three axes of security in this setup:

1. Can the scheduler trust the servers?
2. Is the client permitted to submit and run jobs?
3. Can third parties see and/or modify traffic?

### Server Trust

If a server is malicious, it can return malicious compilation output to a user.
To protect against this, schedulers and servers should authenticate with your
chosen message broker.

RabbitMQ and Redis both support basic-auth:
```toml
# No SSL
message_broker.amqp = "amqp://$AMQP_USERNAME:$AMQP_PASSWORD@$AMQP_URL:5672//"
# With SSL
message_broker.amqp = "amqps://$AMQP_USERNAME:$AMQP_PASSWORD@$AMQP_URL:5671//"
```

```toml
# No SSL
message_broker.redis = "redis://$REDIS_USERNAME:$REDIS_PASSWORD@$REDIS_URL:6379"
# With SSL
message_broker.redis = "rediss://$REDIS_USERNAME:$REDIS_PASSWORD@$REDIS_URL:6379"
```

RabbitMQ also [supports LDAP](https://www.rabbitmq.com/docs/ldap) with a plugin,
but this may not be supported in all cloud service providers' managed offerings.

### Client Trust

If a client is malicious, they can cause a DoS of distributed sccache servers or
explore ways to escape the build sandbox. To protect against this, clients must
be authenticated.

Each client will use an authentication token for the initial job allocation request
to the scheduler. A successful allocation will return a job token that is used
to authorise requests to the appropriate server for that specific job.

This job token is a JWT HS256 token of the job id, signed with a server key.
The key for each server is randomly generated on server startup and given to
the scheduler during registration. This means that the server can verify users
without either a) adding client authentication to every server or b) needing
secret transfer between scheduler and server on every job allocation.

#### OAuth2

This is a group of similar methods for achieving the same thing - the client
retrieves a token from an OAuth2 service, and then submits it to the scheduler
which has a few different options for performing validation on that token.

*To use it*:

Put one of the following settings in your scheduler config file to determine how
the scheduler will validate tokens from the client:

```
# Use the known settings for Mozilla OAuth2 token validation
client_auth = { type = "mozilla" }

# Will forward the valid JWT token onto another URL in the `Bearer` header, with a
# success response indicating the token is valid. Optional `cache_secs` how long
# to cache successful authentication for.
client_auth = { type = "proxy_token", url = "...", cache_secs = 60 }
```

Additionally, each client should set up an OAuth2 configuration in the with one of
the following settings (as appropriate for your OAuth service):

```
# Use the known settings for Mozilla OAuth2 authentication
auth = { type = "mozilla" }

# Use the Authorization Code with PKCE flow. This requires a client id,
# an initial authorize URL (which may have parameters like 'audience' depending
# on your service) and the URL for retrieving a token after the browser flow.
auth = { type = "oauth2_code_grant_pkce", client_id = "...", auth_url = "...", token_url = "..." }

# Use the Implicit flow (typically not recommended due to security issues). This requires
# a client id and an authorize URL (which may have parameters like 'audience' depending
# on your service).
auth = { type = "oauth2_implicit", client_id = "...", auth_url = "..." }
```

The client should then run `sccache --dist-auth` and follow the instructions to retrieve
a token. This will be automatically cached locally for the token expiry period (manual
revalidation will be necessary after expiry).

#### Token

This method simply shares a token between the scheduler and all clients. A token
leak from anywhere allows any attacker to participate as a client.

*To use it*:

Choose a 'secure token' you can share between your scheduler and all clients.

Put the following in your scheduler config file:

```toml
client_auth = { type = "token", token = "YOUR_TOKEN_HERE" }
```

Put the following in your client config file:

```toml
auth = { type = "token", token = "YOUR_TOKEN_HERE" }
```

Done!

#### Insecure (bad idea)

*This route is not recommended*

This method uses a hardcoded token that effectively disables authentication and
provides no security at all.

*To use it*:

Put the following in your scheduler config file:

```toml
client_auth = { type = "DANGEROUSLY_INSECURE" }
```

Remove any `auth =` setting under the `[dist]` heading in your client config file
(it will default to this insecure mode).

Done!

### Eavesdropping and Tampering Protection

If third parties can see traffic to the servers, source code can be leaked. If third
parties can modify traffic to and from the servers or the scheduler, they can cause
the client to receive malicious compiled objects.

Securing communication with the scheduler is the responsibility of the sccache cluster
administrator - it is recommended to put a webserver with a HTTPS certificate in front
of the scheduler and instruct clients to configure their `scheduler_url` with the
appropriate `https://` address.

## Configuration

Use the `--config` argument to pass the path to its configuration file to `sccache-dist`.


### scheduler.toml

```toml
# Id of this scheduler in the message broker's queue names
scheduler_id = "$INSTANCE_ID"

# The socket address the scheduler will listen on. It's strongly recommended
# to listen on localhost and put a HTTPS server in front of it.
public_addr = "127.0.0.1:10600"

# The maximum time (in seconds) that a client should wait for a job to finish.
job_time_limit = 600

# The URL used to connect to the message broker.
# This should be the same as in the server config.
message_broker.amqp = "amqps://$AMQP_USERNAME:$AMQP_PASSWORD@$AMQP_URL:5671//"

[client_auth]
type = "token"
token = "my client token"

# Job inputs/result storage. Must match server configuration.
# Can be any supported storage type (see Configuration.md)
[jobs.s3]
bucket = "$AWS_BUCKET"
region = "$AWS_REGION"
use_ssl = true
no_credentials = false
key_prefix = "$S3_JOBS_KEY"

# Toolchains storage. Must match server configuration.
# Can be any supported storage type (see Configuration.md)
[toolchains.s3]
bucket = "$AWS_BUCKET"
region = "$AWS_REGION"
use_ssl = true
no_credentials = false
key_prefix = "$S3_TOOLCHAINS_KEY"
```


#### [client_auth]

The `[client_auth]` section can be one of (sorted by authentication method):
```toml
# OAuth2
[client_auth]
type = "mozilla"

[client_auth]
type = "proxy_token"
url = "..."
cache_secs = 60

# JWT
[client_auth]
type = "jwt_validate"
audience = "audience"
issuer = "issuer"
jwks_url = "..."

# Token
[client_auth]
type = "token"
token = "preshared token"

# None
[client_auth]
type = "DANGEROUSLY_INSECURE"
```


### server.toml


```toml
# Id of this server in \`sccache --dist-status\`
server_id = "$INSTANCE_ID"

# This is where client toolchains will be stored.
cache_dir = "/tmp/toolchains"
# The maximum size of the toolchain cache, in bytes.
# If unspecified the default is 10GB.
# toolchain_cache_size = 10737418240

# The URL used to connect to the message broker.
# This should be the same as in the scheduler config.
message_broker.amqp = "amqps://$AMQP_USERNAME:$AMQP_PASSWORD@$AMQP_URL:5671//"

[builder]
type = "overlay"
# The directory under which a sandboxed filesystem will be created for builds.
build_dir = "/tmp/build"
# The path to the bubblewrap version 0.3.0+ `bwrap` binary.
bwrap_path = "/usr/bin/bwrap"

# Job inputs/result storage. Must match scheduler configuration.
# Can be any supported storage type (see Configuration.md)
[jobs.s3]
bucket = "$AWS_BUCKET"
region = "$AWS_REGION"
use_ssl = true
no_credentials = false
key_prefix = "$S3_JOBS_KEY"

# Toolchains storage. Must match scheduler configuration.
# Can be any supported storage type (see Configuration.md)
[toolchains.s3]
bucket = "$AWS_BUCKET"
region = "$AWS_REGION"
use_ssl = true
no_credentials = false
key_prefix = "$S3_TOOLCHAINS_KEY"

```


#### [builder]

The `[builder]` section can be can be one of:
```toml
[builder]
type = "docker"

[builder]
type = "overlay"
# The directory under which a sandboxed filesystem will be created for builds.
build_dir = "/tmp/build"
# The path to the bubblewrap version 0.3.0+ `bwrap` binary.
bwrap_path = "/usr/bin/bwrap"

[builder]
type = "pot"
# Pot filesystem root
#pot_fs_root = "/opt/pot"
# Reference pot cloned when creating containers
#clone_from = "sccache-template"
# Command to invoke when calling pot
#pot_cmd = "pot"
# Arguments passed to `pot clone` command
#pot_clone_args = ["-i", "lo0|127.0.0.2"]

```


# Building the Distributed Server Binaries

Until these binaries [are included in releases](https://github.com/mozilla/sccache/issues/393) I've put together a Docker container that can be used to easily build a release binary:
```
docker run -ti --rm -v $PWD:/sccache luser/sccache-musl-build:0.1 /bin/bash -c "cd /sccache; cargo build --release --target x86_64-unknown-linux-musl --features=dist-server && strip target/x86_64-unknown-linux-musl/release/sccache-dist && cd target/x86_64-unknown-linux-musl/release/ && tar czf sccache-dist.tar.gz sccache-dist"
```

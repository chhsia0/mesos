// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "resource_provider/storage/provider.hpp"

#include <glog/logging.h>

#include <process/after.hpp>
#include <process/defer.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/process.hpp>
#include <process/timeout.hpp>

#include <mesos/resource_provider/resource_provider.hpp>

#include <mesos/v1/resource_provider.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>

#include "common/http.hpp"

#include "csi/client.hpp"
#include "csi/utils.hpp"

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "resource_provider/detector.hpp"

#include "resource_provider/storage/paths.hpp"

#include "slave/paths.hpp"

namespace http = process::http;

using std::queue;
using std::shared_ptr;
using std::string;

using process::Break;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::Promise;
using process::Timeout;

using process::after;
using process::defer;
using process::loop;
using process::spawn;
using process::terminate;
using process::wait;

using process::http::authentication::Principal;

using mesos::ResourceProviderInfo;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using mesos::v1::resource_provider::Driver;

namespace mesos {
namespace internal {

static const string SLRP_NAME_PREFIX = "mesos-slrp-";

// We use percent-encoding to escape '.' (reserved for standalone
// containers) and '-' (reserved as a separator) for names, in addition
// to the characters reserved in RFC 3986.
static const string SLRP_NAME_RESERVED = ".-";

// Timeout for a CSI plugin to create its endpoint socket.
// TODO(chhsiao): Make the timeout configurable.
static const Duration CSI_ENDPOINT_CREATION_TIMEOUT = Seconds(5);


// Returns a prefix for naming components of the resource provider. This
// can be used in various places, such as container IDs for CSI plugins.
static inline string getPrefix(const ResourceProviderInfo& info)
{
  return SLRP_NAME_PREFIX + http::encode(info.name(), SLRP_NAME_RESERVED) + "-";
}


// Returns the container ID for the plugin.
static inline ContainerID getContainerID(
    const ResourceProviderInfo& info,
    const string& plugin)
{
  ContainerID containerId;

  containerId.set_value(
      getPrefix(info) + http::encode(plugin, SLRP_NAME_RESERVED));

  return containerId;
}


// Returns the parent endpoint as a URL.
static inline http::URL extractParentEndpoint(const http::URL& url)
{
  http::URL parent = url;

  parent.path = Path(url.path).dirname();

  return parent;
}


// Returns the 'Bearer' credential as a header for calling the V1 agent
// API if the `authToken` is presented, or empty otherwise.
static inline http::Headers getAuthHeader(const Option<string>& authToken)
{
  http::Headers headers;

  if (authToken.isSome()) {
    headers["Authorization"] = "Bearer " + authToken.get();
  }

  return headers;
}


// Convenient function to print an error message with `std::bind`.
static inline void err(const string& message, const string& error)
{
  LOG(ERROR) << message << ": " << error;
}


// Convenient function to check if a container contains a value.
template <typename Iterable, typename Value>
static inline bool contains(const Iterable& iterable, const Value& value)
{
  foreach (const auto& item, iterable) {
    if (item == value) {
      return true;
    }
  }

  return false;
}


class StorageLocalResourceProviderProcess
  : public Process<StorageLocalResourceProviderProcess>
{
public:
  explicit StorageLocalResourceProviderProcess(
      const http::URL& _url,
      const string& workDir,
      const ResourceProviderInfo& _info,
      const SlaveID& _slaveId,
      const Option<string>& _authToken)
    : ProcessBase(process::ID::generate("storage-local-resource-provider")),
      url(_url),
      metaDir(slave::paths::getMetaRootDir(workDir)),
      resourceProviderAgentDir(slave::paths::getResourceProviderAgentRootDir(
          workDir,
          _info.type(),
          _info.name())),
      contentType(ContentType::PROTOBUF),
      info(_info),
      slaveId(_slaveId),
      authToken(_authToken) {}

  StorageLocalResourceProviderProcess(
      const StorageLocalResourceProviderProcess& other) = delete;

  StorageLocalResourceProviderProcess& operator=(
      const StorageLocalResourceProviderProcess& other) = delete;

  void connected();
  void disconnected();
  void received(const Event& event);

private:
  void initialize() override;
  void fatal(const std::string& failure);

  // Functions for received events.
  void subscribed(const Event::Subscribed& subscribed);
  void operation(const Event::Operation& operation);
  void publish(const Event::Publish& publish);

  Future<csi::Client> connect(const string& plugin);
  Future<csi::Client> launch(const string& plugin);
  Future<Nothing> kill(const string& plugin);

  const http::URL url;
  const string metaDir;
  const string resourceProviderAgentDir;
  const ContentType contentType;
  ResourceProviderInfo info;
  const SlaveID slaveId;
  const Option<string> authToken;

  csi::Version csiVersion;
  process::grpc::client::Runtime runtime;
  Owned<v1::resource_provider::Driver> driver;
};


void StorageLocalResourceProviderProcess::connected()
{
  const string error =
    "Failed to subscribe resource provider with type '" + info.type() +
    "' and name '" + info.name();

  Call call;
  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();
  subscribe->mutable_resource_provider_info()->CopyFrom(info);

  driver->send(evolve(call))
    .onFailed(std::bind(err, error, lambda::_1))
    .onDiscarded(std::bind(err, error, "future discarded"));
}


void StorageLocalResourceProviderProcess::disconnected()
{
}


void StorageLocalResourceProviderProcess::received(const Event& event)
{
  LOG(INFO) << "Received " << event.type() << " event";

  switch (event.type()) {
    case Event::SUBSCRIBED: {
      CHECK(event.has_subscribed());
      subscribed(event.subscribed());
      break;
    }
    case Event::OPERATION: {
      CHECK(event.has_operation());
      operation(event.operation());
      break;
    }
    case Event::PUBLISH: {
      CHECK(event.has_publish());
      publish(event.publish());
      break;
    }
    case Event::UNPUBLISH: {
      CHECK(event.has_unpublish());
      break;
    }
    case Event::UNKNOWN: {
      LOG(WARNING) << "Received an UNKNOWN event and ignored";
      break;
    }
  }
}


void StorageLocalResourceProviderProcess::initialize()
{
  const string latest = slave::paths::getLatestResourceProviderPath(
      metaDir,
      slaveId,
      info.type(),
      info.name());

  // Recover the resource provider ID from the latest symlink. If the
  // symlink cannot be resolved, treat this as a new resource provider.
  Result<string> realpath = os::realpath(latest);
  if (realpath.isSome()) {
    info.mutable_id()->set_value(Path(realpath.get()).basename());
  }

  // Prepare the directory where the mount points will be placed.
  const string volumesDir =
    storage::paths::getCsiVolumeRootDir(resourceProviderAgentDir);
  Try<Nothing> mkdir = os::mkdir(volumesDir);
  if (mkdir.isError()) {
    fatal("Failed to create directory '" + volumesDir + "': " + mkdir.error());
  }

  // Set CSI version to 0.1.0.
  csiVersion.set_major(0);
  csiVersion.set_minor(1);
  csiVersion.set_patch(0);

  driver.reset(new Driver(
      Owned<EndpointDetector>(new ConstantEndpointDetector(url)),
      contentType,
      defer(self(), &Self::connected),
      defer(self(), &Self::disconnected),
      defer(self(), [this](queue<v1::resource_provider::Event> events) {
        while(!events.empty()) {
        const v1::resource_provider::Event& event = events.front();
          received(devolve(event));
          events.pop();
        }
      }),
      None())); // TODO(nfnt): Add authentication as part of MESOS-7854.
}


void StorageLocalResourceProviderProcess::fatal(const string& failure)
{
  LOG(ERROR) << failure;
  terminate(self());
}


void StorageLocalResourceProviderProcess::subscribed(
    const Event::Subscribed& subscribed)
{
  LOG(INFO) << "Subscribed with ID " << subscribed.provider_id().value();

  if (!info.has_id()) {
    // New subscription.
    info.mutable_id()->CopyFrom(subscribed.provider_id());
    slave::paths::createResourceProviderDirectory(
        metaDir,
        slaveId,
        info.type(),
        info.name(),
        info.id());
  }
}


void StorageLocalResourceProviderProcess::operation(
    const Event::Operation& operation)
{
}


void StorageLocalResourceProviderProcess::publish(const Event::Publish& publish)
{
}


// Connects to the plugin, or launches it if it is not running.
Future<csi::Client> StorageLocalResourceProviderProcess::connect(
    const string& plugin)
{
  // The plugin bootstrap works as follows:
  // 1. Launch a container with the `CSI_ENDPOINT` environment varible
  //    set to `unix://<endpoint_path>`.
  // 2. Wait for the endpoint socket to appear. The plugin should create
  // and bind to the endpoint socket.
  // 3. Establish a connection to the endpoint socket once it exists.
  // However, the above procedure only works if the endpoint socket does
  // not exist at the beginning. If the endpoint socket alreay exists,
  // we call `GetSupportedVersions` to the endpoint socket first to
  // check if the plugin is already running. If not, we unlink the
  // endpoint socket file before bootstrapping the plugin.

  shared_ptr<Option<csi::Client>> client(new Option<csi::Client>);
  Promise<csi::GetSupportedVersionsResponse> connection;

  Try<string> endpoint =
    storage::paths::getCsiEndpointPath(resourceProviderAgentDir, plugin);

  if (endpoint.isSome() && os::exists(endpoint.get())) {
    *client = csi::Client("unix://" + endpoint.get(), runtime);
    connection.associate((*client)->GetSupportedVersions(
        csi::GetSupportedVersionsRequest::default_instance()));
  } else {
    connection.fail("Socket file not found");
  }

  return connection.future()
    .repair(defer(self(), [=](
        const Future<csi::GetSupportedVersionsResponse>& future)
        -> Future<csi::GetSupportedVersionsResponse> {
      Promise<Nothing> bootstrap;

      if (endpoint.isSome() && os::exists(endpoint.get())) {
        // Try to kill the plugin and remove the endponit socket to
        // ensure that it is not running.
        bootstrap.associate(kill(plugin));
      } else {
        bootstrap.set(Nothing());
      }

      return bootstrap.future()
        .then(defer(self(), &Self::launch, plugin))
        .then(defer(self(), [=](csi::Client _client) {
          *client = _client;
          return _client.GetSupportedVersions(
              csi::GetSupportedVersionsRequest::default_instance());
        }));
    }))
    .then(defer(self(), [=](const csi::GetSupportedVersionsResponse& response)
        -> Future<csi::Client> {
      CHECK_SOME(*client);

      if (contains(response.result().supported_versions(), csiVersion)) {
        return client->get();
      }

      return Failure(
          "CSI version " + stringify(csiVersion) + " is not supported");
    }));
}


// Launches a container for the plugin and waits for its endpoint socket
// file to appear, with a timeout specified by
// `CSI_ENDPOINT_CREATION_TIMEOUT`. The endpoint socket file should not
// exist prior to launch.
Future<csi::Client> StorageLocalResourceProviderProcess::launch(
    const string& plugin)
{
  Try<string> endpoint =
    storage::paths::getCsiEndpointPath(resourceProviderAgentDir, plugin);
  if (endpoint.isError()) {
    return Failure(
        "Failed to resolve endpoint path for plugin '" + plugin + "': " +
        endpoint.error());
  }
  CHECK(!os::exists(endpoint.get()));

  const string& endpointPath = endpoint.get();
  const string endpointDir = Path(endpointPath).dirname();
  const string volumesDir =
    storage::paths::getCsiVolumeRootDir(resourceProviderAgentDir);

  const ContainerID containerId = getContainerID(info, plugin);

  Option<CSIPluginInfo> config;
  foreach (const CSIPluginInfo& _config, info.storage().csi_plugins()) {
    if (_config.name() == plugin) {
      config = _config;
      break;
    }
  }
  CHECK_SOME(config);

  agent::Call call;
  call.set_type(agent::Call::LAUNCH_CONTAINER);

  agent::Call::LaunchContainer* launch = call.mutable_launch_container();
  launch->mutable_container_id()->CopyFrom(containerId);
  launch->mutable_resources()->CopyFrom(config->resources());

  ContainerInfo* container = launch->mutable_container();

  if (config->has_container()) {
    container->CopyFrom(config->container());
  } else {
    container->set_type(ContainerInfo::MESOS);
  }

  // Prepare a volume where the endpoint socket will be placed.
  Volume* endpointVolume = container->add_volumes();
  endpointVolume->set_mode(Volume::RW);
  endpointVolume->set_container_path(endpointDir);
  endpointVolume->set_host_path(endpointDir);

  // Prepare a volume where the mount points will be placed.
  Volume* mountVolume = container->add_volumes();
  mountVolume->set_mode(Volume::RW);
  mountVolume->set_container_path(volumesDir);
  mountVolume->mutable_source()->set_type(Volume::Source::HOST_PATH);
  mountVolume->mutable_source()->mutable_host_path()->set_path(volumesDir);
  mountVolume->mutable_source()->mutable_host_path()
    ->mutable_mount_propagation()->set_mode(MountPropagation::BIDIRECTIONAL);

  CommandInfo* command = launch->mutable_command();

  if (config->has_command()) {
    command->CopyFrom(config->command());
  }

  // Set the `CSI_ENDPOINT` environment variable.
  Environment::Variable* endpointVar =
    command->mutable_environment()->add_variables();
  endpointVar->set_name("CSI_ENDPOINT");
  endpointVar->set_value("unix://" + endpointPath);

  // Launch the plugin and wait for the endpoint socket.
  return http::post(
      extractParentEndpoint(url),
      getAuthHeader(authToken),
      internal::serialize(contentType, evolve(call)),
      stringify(contentType))
    .then(defer(self(), [=](
        const http::Response& response) -> Future<csi::Client> {
      if (response.status != http::OK().status &&
          response.status != http::Accepted().status) {
        return Failure(
            "Failed to launch container '" + stringify(containerId) +
            "': Unexpected response '" + response.status + "' (" +
            response.body + ")");
      }

      Timeout timeout = Timeout::in(CSI_ENDPOINT_CREATION_TIMEOUT);

      return loop(
          self(),
          [=]() -> Future<Nothing> {
            if (timeout.expired()) {
              return Failure(
                  "Timed out waiting for plugin to create endpoint '" +
                  endpointPath + "'");
            }

            return after(Milliseconds(10));
          },
          [=](const Nothing&) -> ControlFlow<csi::Client> {
            if (!os::exists(endpointPath)) {
              return Continue();
            }

            return Break(csi::Client("unix://" + endpointPath, runtime));
          });
    }));
}


// Kills the container for the plugin and remove its endpoint socket
// after its termination, or removes the endpoint socket if the
// container does not exist.
Future<Nothing> StorageLocalResourceProviderProcess::kill(
    const string& plugin)
{
  const ContainerID containerId = getContainerID(info, plugin);

  agent::Call call;
  call.set_type(agent::Call::KILL_CONTAINER);
  call.mutable_kill_container()->mutable_container_id()->CopyFrom(containerId);

  return http::post(
      extractParentEndpoint(url),
      getAuthHeader(authToken),
      internal::serialize(contentType, evolve(call)),
      stringify(contentType))
    .then(defer(self(), [=](const http::Response& response) -> Future<Nothing> {
      if (response.status == http::NotFound().status) {
        return Nothing();
      }

      if (response.status != http::OK().status) {
        return Failure(
            "Failed to kill container '" + stringify(containerId) +
            "': Unexpected response '" + response.status + "' (" +
            response.body + ")");
      }

      // Wait for the termination of the container.
      agent::Call call;
      call.set_type(agent::Call::WAIT_CONTAINER);
      call.mutable_wait_container()->mutable_container_id()
        ->CopyFrom(containerId);

      return http::post(
          extractParentEndpoint(url),
          getAuthHeader(authToken),
          internal::serialize(contentType, evolve(call)),
          stringify(contentType))
        .then(defer(self(), [=](
            const http::Response& response) -> Future<Nothing> {
          if (response.status == http::NotFound().status) {
            return Nothing();
          }

          if (response.status != http::OK().status) {
            return Failure(
                "Failed to wait for container '" + stringify(containerId) +
                "': Unexpected response '" + response.status + "' (" +
                response.body + ")");
          }

          return Nothing();
        }));
    }))
  .then(defer(self(), [=]() -> Future<Nothing> {
    Try<string> endpoint = storage::paths::getCsiEndpointPath(
        resourceProviderAgentDir,
        plugin);

    if (endpoint.isSome() && os::exists(endpoint.get())) {
      Try<Nothing> rm = os::rm(endpoint.get());
      if (rm.isError()) {
        return Failure(
            "Failed to remove endpoint '" + endpoint.get() + "': " +
            rm.error());
      }
    }

    return Nothing();
  }));
}


Try<Owned<LocalResourceProvider>> StorageLocalResourceProvider::create(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const SlaveID& slaveId,
    const Option<string>& authToken)
{
  if (!info.has_storage()) {
    return Error("'ResourceProviderInfo.storage' must be set");
  }

  bool hasControllerPlugin = false;
  bool hasNodePlugin = false;

  foreach (const CSIPluginInfo& plugin, info.storage().csi_plugins()) {
    if (plugin.name() == info.storage().controller_plugin()) {
      hasControllerPlugin = true;
    }
    if (plugin.name() == info.storage().node_plugin()) {
      hasNodePlugin = true;
    }
  }

  if (!hasControllerPlugin) {
    return Error(
        "'" + info.storage().controller_plugin() + "' not found in "
        "'ResourceProviderInfo.storage.csi_plugins'");
  }

  if (!hasNodePlugin) {
    return Error(
        "'" + info.storage().node_plugin() + "' not found in "
        "'ResourceProviderInfo.storage.csi_plugins'");
  }

  return Owned<LocalResourceProvider>(
      new StorageLocalResourceProvider(url, workDir, info, slaveId, authToken));
}


Try<Principal> StorageLocalResourceProvider::principal(
    const ResourceProviderInfo& info)
{
  return Principal(Option<string>::none(), {{"cid_prefix", getPrefix(info)}});
}


StorageLocalResourceProvider::StorageLocalResourceProvider(
    const http::URL& url,
    const string& workDir,
    const ResourceProviderInfo& info,
    const SlaveID& slaveId,
    const Option<string>& authToken)
  : process(new StorageLocalResourceProviderProcess(
        url, workDir, info, slaveId, authToken))
{
  spawn(CHECK_NOTNULL(process.get()));
}


StorageLocalResourceProvider::~StorageLocalResourceProvider()
{
  terminate(process.get());
  wait(process.get());
}

} // namespace internal {
} // namespace mesos {

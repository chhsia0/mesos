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

#include <stdlib.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/util/message_differencer.h>

#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>

#include <mesos/csi/csi.hpp>

#include <stout/hashmap.hpp>
#include <stout/stringify.hpp>

using namespace mesos::csi;

using std::cout;
using std::endl;
using std::find;
using std::string;
using std::unique_ptr;

using google::protobuf::RepeatedPtrField;

using google::protobuf::util::MessageDifferencer;

using grpc::InsecureServerCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace mesos {
namespace internal {
namespace tests {

#define CHECK_REQUIREMENT(req)                                            \
  do {                                                                    \
    if (!(req)) {                                                         \
      response->mutable_error()->mutable_general_error()                  \
        ->set_error_code(Error::GeneralError::MISSING_REQUIRED_FIELD);    \
      return Status::OK;                                                  \
    }                                                                     \
  } while(0)

#define CHECK_REQUIRED_FIELD(base, field)                                 \
  CHECK_REQUIREMENT((base)->has_##field())

#define CHECK_REQUIRED_STRING_FIELD(base, field)                          \
  CHECK_REQUIREMENT(!(base)->field().empty())

#define CHECK_REQUIRED_MAP_FIELD(base, field)                             \
  CHECK_REQUIREMENT(!(base)->field().empty())

#define CHECK_REQUIRED_ONEOF_FIELD(base, field, not_set)                  \
  CHECK_REQUIREMENT((base)->field##_case() != not_set)

#define CHECK_REQUIRED_REPEATED_FIELD(base, field)                        \
  CHECK_REQUIREMENT((base)->field##_size() > 0)


class MockCSIPlugin :
  public Identity::Service,
  public Controller::Service,
  public Node::Service
{
public:
  MockCSIPlugin(
      const string& _name,
      const string& _vendorVersion,
      const string& _addr)
    : name(_name), vendorVersion(_vendorVersion), addr(_addr) {}

  void run()
  {
    ServerBuilder builder;
    builder.AddListeningPort(addr, InsecureServerCredentials());
    builder.RegisterService(static_cast<Identity::Service*>(this));
    builder.RegisterService(static_cast<Controller::Service*>(this));
    unique_ptr<Server> server(builder.BuildAndStart());
    cout << name << " v" << vendorVersion << " listening on " << addr << endl;
    server->Wait();
  }

  void addSupportedVersion(uint32_t major, uint32_t minor, uint32_t patch)
  {
    Version* ver = supportedVersions.Add();
    ver->set_major(major);
    ver->set_minor(minor);
    ver->set_patch(patch);
  }

  void addControllerCapability(ControllerServiceCapability::RPC::Type type)
  {
    ControllerServiceCapability* cap = controllerCapabilities.Add();
    cap->mutable_rpc()->set_type(type);
  }

  void addNodeCapability(NodeServiceCapability::RPC::Type type)
  {
    NodeServiceCapability* cap = nodeCapabilities.Add();
    cap->mutable_rpc()->set_type(type);
  }

  void addBlockVolumeCapability(VolumeCapability::BlockVolume type)
  {
    ControllerServiceCapability* ccap = controllerCapabilities.Add();
    ccap->mutable_volume_capability()->mutable_block()->CopyFrom(type);
    NodeServiceCapability* ncap = nodeCapabilities.Add();
    ncap->mutable_volume_capability()->mutable_block()->CopyFrom(type);
  }

  void addMountVolumeCapability(VolumeCapability::MountVolume type)
  {
    ControllerServiceCapability* ccap = controllerCapabilities.Add();
    ccap->mutable_volume_capability()->mutable_mount()->CopyFrom(type);
    NodeServiceCapability* ncap = nodeCapabilities.Add();
    ncap->mutable_volume_capability()->mutable_mount()->CopyFrom(type);
  }

  virtual Status GetSupportedVersions(
      ServerContext* context,
      const GetSupportedVersionsRequest* request,
      GetSupportedVersionsResponse* response) override
  {
    response->mutable_result()->mutable_supported_versions()
      ->CopyFrom(supportedVersions);

    return Status::OK;
  }

  virtual Status GetPluginInfo(
      ServerContext* context,
      const GetPluginInfoRequest* request,
      GetPluginInfoResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      response->mutable_result()->set_name(name);
      response->mutable_result()->set_vendor_version(vendorVersion);
    }

    return Status::OK;
  }

  virtual Status CreateVolume(
      ServerContext* context,
      const CreateVolumeRequest* request,
      CreateVolumeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_REPEATED_FIELD(request, volume_capabilities);
    for (auto it = request->volume_capabilities().begin();
         it != request->volume_capabilities().end();
         ++it) {
      CHECK_REQUIRED_ONEOF_FIELD(it, value, VolumeCapability::VALUE_NOT_SET);
    }

    if (isSupportedVersion(request->version(), response)) {
      const string& volumeName = request->name();

      if (volumeName.empty()) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(Error::CreateVolumeError::INVALID_VOLUME_NAME);
      } else if (volumes.contains(volumeName)) {
        response->mutable_error()->mutable_create_volume_error()
          ->set_error_code(Error::CreateVolumeError::VOLUME_ALREADY_EXISTS);
      } else {
        VolumeStatus& volume = volumes[volumeName];
        volume.state = VOLUME_STATE_PROVISIONED;

        response->mutable_result()->mutable_volume_info()
          ->mutable_access_mode()->set_mode(AccessMode::SINGLE_NODE_WRITER);
        (*response->mutable_result()->mutable_volume_info()
          ->mutable_id()->mutable_values())["name"] = volumeName;
      }
    }

    return Status::OK;
  }

  virtual Status DeleteVolume(
      ServerContext* context,
      const DeleteVolumeRequest* request,
      DeleteVolumeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_id().values().find("name");

      if (vit == request->volume_id().values().end() || vit->second.empty()) {
        response->mutable_error()->mutable_delete_volume_error()
          ->set_error_code(Error::DeleteVolumeError::INVALID_VOLUME_ID);
      } else if (!volumes.contains(vit->second)){
        response->mutable_error()->mutable_delete_volume_error()
          ->set_error_code(Error::DeleteVolumeError::VOLUME_DOES_NOT_EXIST);
      } else {
        // TODO(chhsiao): check volume status.
        volumes.erase(vit->second);
        response->mutable_result();
      }
    }

    return Status::OK;
  }

  virtual Status ControllerPublishVolume(
      ServerContext* context,
      const ControllerPublishVolumeRequest* request,
      ControllerPublishVolumeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
    CHECK_REQUIRED_FIELD(request, node_id);
    CHECK_REQUIRED_MAP_FIELD(&request->node_id(), values);

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_id().values().find("name");
      auto nit = request->node_id().values().find("addr");

      if (vit == request->volume_id().values().end() || vit->second.empty()) {
        response->mutable_error()
          ->mutable_controller_publish_volume_volume_error()
          ->set_error_code(Error::ControllerPublishVolumeError::
            INVALID_VOLUME_ID);
      } else if (!volumes.contains(vit->second)) {
        response->mutable_error()
          ->mutable_controller_publish_volume_volume_error()
          ->set_error_code(Error::ControllerPublishVolumeError::
            VOLUME_DOES_NOT_EXIST);
      } else if (volumes[vit->second].state != VOLUME_STATE_PROVISIONED) {
        response->mutable_error()
          ->mutable_controller_publish_volume_volume_error()
          ->set_error_code(Error::ControllerPublishVolumeError::
            VOLUME_ALREADY_ATTACHED);
      } else if (nit == request->node_id().values().end()) {
        // TODO(chhsiao): check node existence.
        response->mutable_error()
          ->mutable_controller_publish_volume_volume_error()
          ->set_error_code(Error::ControllerPublishVolumeError::
            NODE_DOES_NOT_EXIST);
      } else {
        VolumeStatus& volume = volumes[vit->second];
        volume.state = VOLUME_STATE_ATTACHED;
        volume.node = nit->second;

        response->mutable_result();
      }
    }

    return Status::OK;
  }

  virtual Status ControllerUnpublishVolume(
      ServerContext* context,
      const ControllerUnpublishVolumeRequest* request,
      ControllerUnpublishVolumeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
    CHECK_REQUIRED_FIELD(request, node_id);
    CHECK_REQUIRED_MAP_FIELD(&request->node_id(), values);

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_id().values().find("name");
      auto nit = request->node_id().values().find("addr");

      if (vit == request->volume_id().values().end() || vit->second.empty()) {
        response->mutable_error()
          ->mutable_controller_unpublish_volume_volume_error()
          ->set_error_code(Error::ControllerUnpublishVolumeError::
            INVALID_VOLUME_ID);
      } else if (!volumes.contains(vit->second)) {
        response->mutable_error()
          ->mutable_controller_unpublish_volume_volume_error()
          ->set_error_code(Error::ControllerUnpublishVolumeError::
            VOLUME_DOES_NOT_EXIST);
      } else if (nit == request->node_id().values().end()) {
        // TODO(chhsiao): check node existence.
        response->mutable_error()
          ->mutable_controller_unpublish_volume_volume_error()
          ->set_error_code(Error::ControllerUnpublishVolumeError::
            NODE_DOES_NOT_EXIST);
      } else if (volumes[vit->second].node != nit->second) {
        response->mutable_error()
          ->mutable_controller_unpublish_volume_volume_error()
          ->set_error_code(Error::ControllerUnpublishVolumeError::
            VOLUME_NOT_ATTACHED_TO_SPECIFIED_NODE);
      } else {
        VolumeStatus& volume = volumes[vit->second];
        volume.state = VOLUME_STATE_PROVISIONED;
        volume.node.clear();

        response->mutable_result();
      }
    }

    return Status::OK;
  }

  virtual Status ValidateVolumeCapabilities(
      ServerContext* context,
      const ValidateVolumeCapabilitiesRequest* request,
      ValidateVolumeCapabilitiesResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_info);
    CHECK_REQUIRED_FIELD(&request->volume_info(), access_mode);
    CHECK_REQUIRED_FIELD(&request->volume_info(), id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_info().id(), values);
    CHECK_REQUIRED_REPEATED_FIELD(request, volume_capabilities);
    for (auto it = request->volume_capabilities().begin();
         it != request->volume_capabilities().end();
         ++it) {
      CHECK_REQUIRED_ONEOF_FIELD(it, value, VolumeCapability::VALUE_NOT_SET);
    }

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_info().id().values().find("name");

      if (vit == request->volume_info().id().values().end() ||
          vit->second.empty() ||
          !volumes.contains(vit->second)) {
        // TODO(chhsiao): INVALID_VOLUME_ID?
        response->mutable_error()->mutable_validate_volume_capabilities_error()
          ->set_error_code(Error::ValidateVolumeCapabilitiesError::
            VOLUME_DOES_NOT_EXIST);
      } else {
        response->mutable_result()->set_supported(true);
      }
    }

    return Status::OK;
  }

  virtual Status ListVolumes(
      ServerContext* context,
      const ListVolumesRequest* request,
      ListVolumesResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      uint32_t starting =
        strtoul(request->starting_token().c_str(), nullptr, 0);
      uint32_t capacity = request->max_entries() ? request->max_entries() : -1u;
      uint32_t index = 0;
      auto it = volumes.begin();

      while (index < starting && it != volumes.end()) {
        ++index;
        ++it;
      }

      while (it != volumes.end()) {
        if (response->mutable_result()->entries_size() == capacity) {
          break;
        }

        ListVolumesResponse::Result::Entry* entry =
          response->mutable_result()->add_entries();
        entry->mutable_volume_info()
          ->mutable_access_mode()->set_mode(AccessMode::SINGLE_NODE_WRITER);
        (*entry->mutable_volume_info()
          ->mutable_id()->mutable_values())["name"] = it->first;

        ++index;
      }

      if (it != volumes.end()) {
        response->mutable_result()->set_next_token(stringify(index));
      }
    }

    return Status::OK;
  }

  virtual Status GetCapacity(
      ServerContext* context,
      const GetCapacityRequest* request,
      GetCapacityResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      response->mutable_result()->set_total_capacity(0);
    }

    return Status::OK;
  }

  virtual Status ControllerGetCapabilities(
      ServerContext* context,
      const ControllerGetCapabilitiesRequest* request,
      ControllerGetCapabilitiesResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      response->mutable_result()->mutable_capabilities()
        ->CopyFrom(controllerCapabilities);
    }

    return Status::OK;
  }

  virtual Status NodePublishVolume(
      ServerContext* context,
      const NodePublishVolumeRequest* request,
      NodePublishVolumeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
    CHECK_REQUIRED_STRING_FIELD(request, target_path);
    CHECK_REQUIRED_FIELD(request, volume_capability);
    CHECK_REQUIRED_ONEOF_FIELD(
      &request->volume_capability(), value, VolumeCapability::VALUE_NOT_SET);

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_id().values().find("name");

      if (vit == request->volume_id().values().end() ||
          vit->second.empty() ||
          !volumes.contains(vit->second) ||
          volumes[vit->second].state != VOLUME_STATE_ATTACHED ||
          volumes[vit->second].node != addr) {
        // TODO(chhsiao): INVALID_VOLUME_ID?
        response->mutable_error()->mutable_node_publish_volume_error()
          ->set_error_code(Error::NodePublishVolumeError::
            VOLUME_DOES_NOT_EXIST);
      } else {
        VolumeStatus& volume = volumes[vit->second];
        volume.state = VOLUME_STATE_MOUNTED;
        volume.path = request->target_path();

        response->mutable_result();
      }
    }

    return Status::OK;
  }

  virtual Status NodeUnpublishVolume(
      ServerContext* context,
      const NodeUnpublishVolumeRequest* request,
      NodeUnpublishVolumeResponse* response)
  {
    CHECK_REQUIRED_FIELD(request, version);
    CHECK_REQUIRED_FIELD(request, volume_id);
    CHECK_REQUIRED_MAP_FIELD(&request->volume_id(), values);
    CHECK_REQUIRED_STRING_FIELD(request, target_path);

    if (isSupportedVersion(request->version(), response)) {
      auto vit = request->volume_id().values().find("name");

      if (vit == request->volume_id().values().end() ||
          vit->second.empty() ||
          !volumes.contains(vit->second) ||
          volumes[vit->second].state != VOLUME_STATE_MOUNTED ||
          volumes[vit->second].node != addr ||
          volumes[vit->second].path != request->target_path()) {
        // TODO(chhsiao): INVALID_VOLUME_ID?
        response->mutable_error()->mutable_node_unpublish_volume_error()
          ->set_error_code(Error::NodeUnpublishVolumeError::
            VOLUME_DOES_NOT_EXIST);
      } else {
        VolumeStatus& volume = volumes[vit->second];
        volume.state = VOLUME_STATE_ATTACHED;
        volume.path.clear();

        response->mutable_result();
      }
    }

    return Status::OK;
  }

  virtual Status GetNodeID(
      ServerContext* context,
      const GetNodeIDRequest* request,
      GetNodeIDResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      (*response->mutable_result()->mutable_node_id()
        ->mutable_values())["addr"] = addr;
    }

    return Status::OK;
  }

  virtual Status ProbeNode(
      ServerContext* context,
      const ProbeNodeRequest* request,
      ProbeNodeResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      response->mutable_result();
    }

    return Status::OK;
  }

  virtual Status NodeGetCapabilities(
      ServerContext* context,
      const NodeGetCapabilitiesRequest* request,
      NodeGetCapabilitiesResponse* response) override
  {
    CHECK_REQUIRED_FIELD(request, version);

    if (isSupportedVersion(request->version(), response)) {
      response->mutable_result()->mutable_capabilities()
        ->CopyFrom(nodeCapabilities);
    }

    return Status::OK;
  }

private:
  enum VolumeState
  {
    VOLUME_STATE_VIRGIN,
    VOLUME_STATE_PROVISIONED,
    VOLUME_STATE_ATTACHED,
    VOLUME_STATE_MOUNTED
  };

  struct VolumeStatus
  {
    VolumeStatus() : state(VOLUME_STATE_VIRGIN) {}

    VolumeState state;
    string node;
    string path;
  };

  template <typename Response>
  bool isSupportedVersion(const Version& version, Response* response)
  {
    auto it = find_if(
        supportedVersions.begin(),
        supportedVersions.end(),
        [version](const Version& v) {
          return MessageDifferencer::Equals(version, v);
        });
    if (it == supportedVersions.end()) {
      response->mutable_error()->mutable_general_error()
        ->set_error_code(Error::GeneralError::UNSUPPORTED_REQUEST_VERSION);
      return false;
    }

    return true;
  }

  const string name;
  const string vendorVersion;
  const string addr;
  RepeatedPtrField<Version> supportedVersions;
  RepeatedPtrField<ControllerServiceCapability> controllerCapabilities;
  RepeatedPtrField<NodeServiceCapability> nodeCapabilities;
  hashmap<string, VolumeStatus> volumes;
};


extern "C" int main()
{
  MockCSIPlugin plugin("Mock CSI Plugin", "0", "localhost:50051");
  plugin.run();

  return 0;
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {

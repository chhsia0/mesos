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

#ifndef __CSI_ADAPTOR_HPP__
#define __CSI_ADAPTOR_HPP__

#include <csi/spec.hpp>

#include <process/grpc.hpp>

#include "csi/rpc.hpp"

namespace mesos {
namespace csi {
namespace v0 {

class Adaptor
{
public:
  Adaptor(const process::grpc::Connection& _connection,
         const process::grpc::Client& _client)
    : connection(_connection), client(_client) {}

  template <RPC rpc>
  process::Future<typename RPCTraits<rpc>::response_type> call(
      typename RPCTraits<rpc>::request_type request);

private:
  process::grpc::Connection connection;
  process::grpc::Client client;
};


template <>
process::Future<GetPluginInfoResponse>
Adaptor::call<GET_PLUGIN_INFO>(
    GetPluginInfoRequest request);


template <>
process::Future<GetPluginCapabilitiesResponse>
Adaptor::call<GET_PLUGIN_CAPABILITIES>(
    GetPluginCapabilitiesRequest request);


template <>
process::Future<ProbeResponse>
Adaptor::call<PROBE>(
    ProbeRequest request);


template <>
process::Future<CreateVolumeResponse>
Adaptor::call<CREATE_VOLUME>(
    CreateVolumeRequest request);


template <>
process::Future<DeleteVolumeResponse>
Adaptor::call<DELETE_VOLUME>(
    DeleteVolumeRequest request);


template <>
process::Future<ControllerPublishVolumeResponse>
Adaptor::call<CONTROLLER_PUBLISH_VOLUME>(
    ControllerPublishVolumeRequest request);


template <>
process::Future<ControllerUnpublishVolumeResponse>
Adaptor::call<CONTROLLER_UNPUBLISH_VOLUME>(
    ControllerUnpublishVolumeRequest request);


template <>
process::Future<ValidateVolumeCapabilitiesResponse>
Adaptor::call<VALIDATE_VOLUME_CAPABILITIES>(
    ValidateVolumeCapabilitiesRequest request);


template <>
process::Future<ListVolumesResponse>
Adaptor::call<LIST_VOLUMES>(
    ListVolumesRequest request);


template <>
process::Future<GetCapacityResponse>
Adaptor::call<GET_CAPACITY>(
    GetCapacityRequest request);


template <>
process::Future<ControllerGetCapabilitiesResponse>
Adaptor::call<CONTROLLER_GET_CAPABILITIES>(
    ControllerGetCapabilitiesRequest request);


template <>
process::Future<NodeStageVolumeResponse>
Adaptor::call<NODE_STAGE_VOLUME>(
    NodeStageVolumeRequest request);


template <>
process::Future<NodeUnstageVolumeResponse>
Adaptor::call<NODE_UNSTAGE_VOLUME>(
    NodeUnstageVolumeRequest request);


template <>
process::Future<NodePublishVolumeResponse>
Adaptor::call<NODE_PUBLISH_VOLUME>(
    NodePublishVolumeRequest request);


template <>
process::Future<NodeUnpublishVolumeResponse>
Adaptor::call<NODE_UNPUBLISH_VOLUME>(
    NodeUnpublishVolumeRequest request);


template <>
process::Future<NodeGetIdResponse>
Adaptor::call<NODE_GET_ID>(
    NodeGetIdRequest request);


template <>
process::Future<NodeGetCapabilitiesResponse>
Adaptor::call<NODE_GET_CAPABILITIES>(
    NodeGetCapabilitiesRequest request);

} // namespace v0 {
} // namespace csi {
} // namespace mesos {

#endif // __CSI_ADAPTOR_HPP__

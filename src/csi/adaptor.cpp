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

#include "csi/adaptor.hpp"

#include <utility>

using process::Failure;
using process::Future;

using process::grpc::CallOptions;
using process::grpc::StatusError;

namespace mesos {
namespace csi {
namespace v0 {

template <>
Future<GetPluginInfoResponse>
Adaptor::call<GET_PLUGIN_INFO>(
    GetPluginInfoRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Identity, GetPluginInfo),
        std::move(request),
        CallOptions())
    .then([](const Try<GetPluginInfoResponse, StatusError>& result)
        -> Future<GetPluginInfoResponse> {
      return result;
    });
}


template <>
Future<GetPluginCapabilitiesResponse>
Adaptor::call<GET_PLUGIN_CAPABILITIES>(
    GetPluginCapabilitiesRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Identity, GetPluginCapabilities),
        std::move(request),
        CallOptions())
    .then([](const Try<GetPluginCapabilitiesResponse, StatusError>& result)
        -> Future<GetPluginCapabilitiesResponse> {
      return result;
    });
}


template <>
Future<ProbeResponse>
Adaptor::call<PROBE>(
    ProbeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Identity, Probe),
        std::move(request),
        CallOptions())
    .then([](const Try<ProbeResponse, StatusError>& result)
        -> Future<ProbeResponse> {
      return result;
    });
}


template <>
Future<CreateVolumeResponse>
Adaptor::call<CREATE_VOLUME>(
    CreateVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, CreateVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<CreateVolumeResponse, StatusError>& result)
        -> Future<CreateVolumeResponse> {
      return result;
    });
}


template <>
Future<DeleteVolumeResponse>
Adaptor::call<DELETE_VOLUME>(
    DeleteVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, DeleteVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<DeleteVolumeResponse, StatusError>& result)
        -> Future<DeleteVolumeResponse> {
      return result;
    });
}


template <>
Future<ControllerPublishVolumeResponse>
Adaptor::call<CONTROLLER_PUBLISH_VOLUME>(
    ControllerPublishVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, ControllerPublishVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<ControllerPublishVolumeResponse, StatusError>& result)
        -> Future<ControllerPublishVolumeResponse> {
      return result;
    });
}


template <>
Future<ControllerUnpublishVolumeResponse>
Adaptor::call<CONTROLLER_UNPUBLISH_VOLUME>(
    ControllerUnpublishVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, ControllerUnpublishVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<ControllerUnpublishVolumeResponse, StatusError>& result)
        -> Future<ControllerUnpublishVolumeResponse> {
      return result;
    });
}


template <>
Future<ValidateVolumeCapabilitiesResponse>
Adaptor::call<VALIDATE_VOLUME_CAPABILITIES>(
    ValidateVolumeCapabilitiesRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, ValidateVolumeCapabilities),
        std::move(request),
        CallOptions())
    .then([](const Try<ValidateVolumeCapabilitiesResponse, StatusError>& result)
        -> Future<ValidateVolumeCapabilitiesResponse> {
      return result;
    });
}


template <>
Future<ListVolumesResponse>
Adaptor::call<LIST_VOLUMES>(
    ListVolumesRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, ListVolumes),
        std::move(request),
        CallOptions())
    .then([](const Try<ListVolumesResponse, StatusError>& result)
        -> Future<ListVolumesResponse> {
      return result;
    });
}


template <>
Future<GetCapacityResponse>
Adaptor::call<GET_CAPACITY>(
    GetCapacityRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, GetCapacity),
        std::move(request),
        CallOptions())
    .then([](const Try<GetCapacityResponse, StatusError>& result)
        -> Future<GetCapacityResponse> {
      return result;
    });
}


template <>
Future<ControllerGetCapabilitiesResponse>
Adaptor::call<CONTROLLER_GET_CAPABILITIES>(
    ControllerGetCapabilitiesRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Controller, ControllerGetCapabilities),
        std::move(request),
        CallOptions())
    .then([](const Try<ControllerGetCapabilitiesResponse, StatusError>& result)
        -> Future<ControllerGetCapabilitiesResponse> {
      return result;
    });
}


template <>
Future<NodeStageVolumeResponse>
Adaptor::call<NODE_STAGE_VOLUME>(
    NodeStageVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodeStageVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<NodeStageVolumeResponse, StatusError>& result)
        -> Future<NodeStageVolumeResponse> {
      return result;
    });
}


template <>
Future<NodeUnstageVolumeResponse>
Adaptor::call<NODE_UNSTAGE_VOLUME>(
    NodeUnstageVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodeUnstageVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<NodeUnstageVolumeResponse, StatusError>& result)
        -> Future<NodeUnstageVolumeResponse> {
      return result;
    });
}


template <>
Future<NodePublishVolumeResponse>
Adaptor::call<NODE_PUBLISH_VOLUME>(
    NodePublishVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodePublishVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<NodePublishVolumeResponse, StatusError>& result)
        -> Future<NodePublishVolumeResponse> {
      return result;
    });
}


template <>
Future<NodeUnpublishVolumeResponse>
Adaptor::call<NODE_UNPUBLISH_VOLUME>(
    NodeUnpublishVolumeRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodeUnpublishVolume),
        std::move(request),
        CallOptions())
    .then([](const Try<NodeUnpublishVolumeResponse, StatusError>& result)
        -> Future<NodeUnpublishVolumeResponse> {
      return result;
    });
}


template <>
Future<NodeGetIdResponse>
Adaptor::call<NODE_GET_ID>(
    NodeGetIdRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodeGetId),
        std::move(request),
        CallOptions())
    .then([](const Try<NodeGetIdResponse, StatusError>& result)
        -> Future<NodeGetIdResponse> {
      return result;
    });
}


template <>
Future<NodeGetCapabilitiesResponse>
Adaptor::call<NODE_GET_CAPABILITIES>(
    NodeGetCapabilitiesRequest request)
{
  return client
    .call(
        connection,
        GRPC_CLIENT_METHOD(Node, NodeGetCapabilities),
        std::move(request),
        CallOptions())
    .then([](const Try<NodeGetCapabilitiesResponse, StatusError>& result)
        -> Future<NodeGetCapabilitiesResponse> {
      return result;
    });
}

} // namespace v0 {
} // namespace csi {
} // namespace mesos {

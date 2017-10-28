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

#include "resource_provider/manager.hpp"

#include <glog/logging.h>

#include <string>
#include <utility>

#include <process/collect.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>

#include <stout/error.hpp>
#include <stout/hashmap.hpp>
#include <stout/protobuf.hpp>

#include "common/protobuf_utils.hpp"

#include "internal/devolve.hpp"

#include "resource_provider/manager_process.hpp"
#include "resource_provider/validation.hpp"

namespace http = process::http;

using std::list;
using std::string;

using mesos::internal::resource_provider::validation::call::validate;

using mesos::resource_provider::Call;
using mesos::resource_provider::Event;

using process::Failure;
using process::Future;
using process::Owned;
using process::Process;
using process::ProcessBase;
using process::Promise;
using process::Queue;

using process::collect;
using process::dispatch;
using process::spawn;
using process::terminate;
using process::wait;

using process::http::Accepted;
using process::http::BadRequest;
using process::http::OK;
using process::http::MethodNotAllowed;
using process::http::NotAcceptable;
using process::http::NotImplemented;
using process::http::Pipe;
using process::http::UnsupportedMediaType;

using process::http::authentication::Principal;

namespace mesos {
namespace internal {

ResourceProviderManagerProcess::ResourceProviderManagerProcess()
  : ProcessBase(process::ID::generate("resource-provider-manager"))
{
}


ResourceProviderManagerProcess::~ResourceProviderManagerProcess()
{
}


Future<http::Response> ResourceProviderManagerProcess::api(
    const http::Request& request,
    const Option<Principal>& principal)
{
  if (request.method != "POST") {
    return MethodNotAllowed({"POST"}, request.method);
  }

  v1::resource_provider::Call v1Call;

  // TODO(anand): Content type values are case-insensitive.
  Option<string> contentType = request.headers.get("Content-Type");

  if (contentType.isNone()) {
    return BadRequest("Expecting 'Content-Type' to be present");
  }

  if (contentType.get() == APPLICATION_PROTOBUF) {
    if (!v1Call.ParseFromString(request.body)) {
      return BadRequest("Failed to parse body into Call protobuf");
    }
  } else if (contentType.get() == APPLICATION_JSON) {
    Try<JSON::Value> value = JSON::parse(request.body);
    if (value.isError()) {
      return BadRequest("Failed to parse body into JSON: " + value.error());
    }

    Try<v1::resource_provider::Call> parse =
      ::protobuf::parse<v1::resource_provider::Call>(value.get());

    if (parse.isError()) {
      return BadRequest("Failed to convert JSON into Call protobuf: " +
                        parse.error());
    }

    v1Call = parse.get();
  } else {
    return UnsupportedMediaType(
        string("Expecting 'Content-Type' of ") +
        APPLICATION_JSON + " or " + APPLICATION_PROTOBUF);
  }

  Call call = devolve(v1Call);

  Option<Error> error = validate(call);
  if (error.isSome()) {
    return BadRequest(
        "Failed to validate resource_provider::Call: " + error->message);
  }

  if (call.type() == Call::SUBSCRIBE) {
    // We default to JSON 'Content-Type' in the response since an empty
    // 'Accept' header results in all media types considered acceptable.
    ContentType acceptType = ContentType::JSON;

    if (request.acceptsMediaType(APPLICATION_JSON)) {
      acceptType = ContentType::JSON;
    } else if (request.acceptsMediaType(APPLICATION_PROTOBUF)) {
      acceptType = ContentType::PROTOBUF;
    } else {
      return NotAcceptable(
          string("Expecting 'Accept' to allow ") +
          "'" + APPLICATION_PROTOBUF + "' or '" + APPLICATION_JSON + "'");
    }

    if (request.headers.contains("Mesos-Stream-Id")) {
      return BadRequest(
          "Subscribe calls should not include the 'Mesos-Stream-Id' header");
    }

    Pipe pipe;
    OK ok;

    ok.headers["Content-Type"] = stringify(acceptType);
    ok.type = http::Response::PIPE;
    ok.reader = pipe.reader();

    // Generate a stream ID and return it in the response.
    UUID streamId = UUID::random();
    ok.headers["Mesos-Stream-Id"] = streamId.toString();

    HttpConnection http(pipe.writer(), acceptType, streamId);
    subscribe(http, call.subscribe());

    return ok;
  }

  if (!resourceProviders.contains(call.resource_provider_id())) {
    return BadRequest("Resource provider cannot be found");
  }

  ResourceProvider& resourceProvider =
    resourceProviders.at(call.resource_provider_id());

  // This isn't a `SUBSCRIBE` call, so the request should include a stream ID.
  if (!request.headers.contains("Mesos-Stream-Id")) {
    return BadRequest(
        "All non-subscribe calls should include to 'Mesos-Stream-Id' header");
  }

  const string& streamId = request.headers.at("Mesos-Stream-Id");
  if (streamId != resourceProvider.http.streamId.toString()) {
    return BadRequest(
        "The stream ID '" + streamId + "' included in this request "
        "didn't match the stream ID currently associated with "
        " resource provider ID " + resourceProvider.info.id().value());
  }

  switch(call.type()) {
    case Call::UNKNOWN: {
      return NotImplemented();
    }

    case Call::SUBSCRIBE: {
      // `SUBSCRIBE` call should have been handled above.
      LOG(FATAL) << "Unexpected 'SUBSCRIBE' call";
    }

    case Call::UPDATE_OFFER_OPERATION_STATUS: {
      updateOfferOperationStatus(
          &resourceProvider,
          call.update_offer_operation_status());

      return Accepted();
    }

    case Call::UPDATE_STATE: {
      updateState(&resourceProvider, call.update_state());
      return Accepted();
    }

    case Call::PUBLISHED: {
      published(&resourceProvider, call.published());
      return Accepted();
    }

    case Call::UNPUBLISHED: {
      // TODO(nfnt): Add a 'UNPUBLISHED' handler.
      return NotImplemented();
    }
  }

  UNREACHABLE();
}


void ResourceProviderManagerProcess::apply(
    const FrameworkID& frameworkId,
    const Offer::Operation& operation,
    const UUID& operationUUID)
{
  const Resources& resources = protobuf::getConsumedResources(operation);

  Option<ResourceProviderID> resourceProviderId;

  foreach (const Resource& resource, resources) {
    if (!resource.has_provider_id()) {
      LOG(WARNING) << "Dropping Operation " << operationUUID.toString()
                   << " because a resource does not have a resource provider";
      return;
    }

    if (resourceProviderId.isSome() &&
        resourceProviderId.get() != resource.provider_id()) {
      LOG(WARNING) << "Dropping Operation " << operationUUID.toString()
                   << " because its resources are from multiple"
                   << " resource providers";
      return;
    }

    resourceProviderId = resource.provider_id();
  }

  CHECK_SOME(resourceProviderId);

  if (!resourceProviders.contains(resourceProviderId.get())) {
    LOG(WARNING) << "Dropping Operation " << operationUUID.toString()
                 << " for unknown resource provider "
                 << resourceProviderId.get();
    return;
  }

  ResourceProvider& resourceProvider =
    resourceProviders.at(resourceProviderId.get());

  Event event;
  event.set_type(Event::OPERATION);
  event.mutable_operation()->mutable_framework_id()->CopyFrom(frameworkId);
  event.mutable_operation()->mutable_info()->CopyFrom(operation);
  event.mutable_operation()->set_operation_uuid(operationUUID.toString());

  // TODO(nfnt): Keep track of all resource provider version UUIDs in
  // the manager and set it to the current one here.
  event.mutable_operation()->set_resource_version_uuid(
      UUID::random().toString());

  if (!resourceProvider.http.send(event)) {
    LOG(WARNING) << "Could not send operation to resource provider "
                 << resourceProviderId.get();
  }
}


Future<Nothing> ResourceProviderManagerProcess::publish(
    const SlaveID& slaveId,
    const Resources& resources)
{
  hashmap<ResourceProviderID, Resources> publishing;

  foreach (const Resource& resource, resources) {
    if (resource.has_provider_id()) {
      publishing[resource.provider_id()] += resource;
    }
  }

  list<Future<Nothing>> futures;

  foreachpair (
      const ResourceProviderID& resourceProviderId,
      const Resources& resources,
      publishing) {
    const string uuid = UUID::random().toBytes();
    pending[uuid].reset(new Promise<Nothing>());
    futures.push_back(pending[uuid]->future());

    if (!resourceProviders.contains(resourceProviderId)) {
      // TODO(chhsiao): If the manager is running on an agent and the
      // resource comes from an external resource provider, we may want
      // to load the provider's agent component.
      pending[uuid]->fail(
          "Unknown resource provider " + stringify(resourceProviderId));
    }

    ResourceProvider& resourceProvider =
      resourceProviders.at(resourceProviderId);

    // TODO(chhsiao): Remove the framework ID.
    Event event;
    event.set_type(Event::PUBLISH);
    event.mutable_publish()->set_uuid(uuid);
    event.mutable_publish()->mutable_agent_id()->CopyFrom(slaveId);
    event.mutable_publish()->mutable_framework_id()->set_value("");
    event.mutable_publish()->mutable_resources()->CopyFrom(resources);

    if (!resourceProvider.http.send(event)) {
      pending[uuid]->fail(
        "Could not send operation to resource provider " +
        stringify(resourceProviderId));
    }
  }

  return collect(futures).then([] { return Nothing(); });
}


void ResourceProviderManagerProcess::subscribe(
    const HttpConnection& http,
    const Call::Subscribe& subscribe)
{
  ResourceProviderInfo resourceProviderInfo =
    subscribe.resource_provider_info();

  // TODO(chhsiao): Reject the subscription if it contains an unknown ID
  // or there is already a subscribed instance with the same ID, and add
  // tests for re-subscriptions.
  if (!resourceProviderInfo.has_id()) {
    resourceProviderInfo.mutable_id()->CopyFrom(newResourceProviderId());
  }

  ResourceProvider resourceProvider(resourceProviderInfo, http);

  Event event;
  event.set_type(Event::SUBSCRIBED);
  event.mutable_subscribed()->mutable_provider_id()->CopyFrom(
      resourceProvider.info.id());

  if (!resourceProvider.http.send(event)) {
    LOG(WARNING) << "Unable to send event to resource provider "
                 << stringify(resourceProvider.info.id())
                 << ": connection closed";
  }

  resourceProviders.put(resourceProviderInfo.id(), std::move(resourceProvider));
}


void ResourceProviderManagerProcess::updateOfferOperationStatus(
    ResourceProvider* resourceProvider,
    const Call::UpdateOfferOperationStatus& update)
{
  ResourceProviderMessage message;
  message.type = ResourceProviderMessage::Type::UPDATE_OFFER_OPERATION_STATUS;

  ResourceProviderMessage::UpdateOfferOperationStatus
    updateOfferOperationStatus;
  updateOfferOperationStatus.id = resourceProvider->info.id();
  updateOfferOperationStatus.frameworkId = update.framework_id();
  updateOfferOperationStatus.status = update.status();
  updateOfferOperationStatus.latestStatus = update.latest_status();
  updateOfferOperationStatus.operationUUID = update.operation_uuid();

  message.updateOfferOperationStatus = std::move(updateOfferOperationStatus);
  messages.put(std::move(message));
}


void ResourceProviderManagerProcess::updateState(
    ResourceProvider* resourceProvider,
    const Call::UpdateState& update)
{
  Resources resources;

  foreach (const Resource& resource, update.resources()) {
    CHECK_EQ(resource.provider_id(), resourceProvider->info.id());
    resources += resource;
  }

  resourceProvider->resources = std::move(resources);

  // TODO(chhsiao): Report pending operations.

  ResourceProviderMessage::UpdateTotalResources updateTotalResources;
  updateTotalResources.id = resourceProvider->info.id();
  updateTotalResources.total = resourceProvider->resources;

  ResourceProviderMessage message;
  message.type = ResourceProviderMessage::Type::UPDATE_TOTAL_RESOURCES;
  message.updateTotalResources = std::move(updateTotalResources);

  messages.put(std::move(message));
}


void ResourceProviderManagerProcess::published(
    ResourceProvider* resourceProvider,
    const Call::Published& published)
{
  if (pending.contains(published.uuid())) {
    pending[published.uuid()]->set(Nothing());
    pending.erase(published.uuid());
  }
}


ResourceProviderID ResourceProviderManagerProcess::newResourceProviderId()
{
  ResourceProviderID resourceProviderId;
  resourceProviderId.set_value(UUID::random().toString());
  return resourceProviderId;
}


ResourceProviderManager::ResourceProviderManager()
  : process(new ResourceProviderManagerProcess())
{
  spawn(CHECK_NOTNULL(process.get()));
}


ResourceProviderManager::ResourceProviderManager(
    const Owned<ResourceProviderManagerProcess>& _process)
  : process(_process)
{
  spawn(CHECK_NOTNULL(process.get()));
}


ResourceProviderManager::~ResourceProviderManager()
{
  terminate(process.get());
  wait(process.get());
}


Future<http::Response> ResourceProviderManager::api(
    const http::Request& request,
    const Option<Principal>& principal) const
{
  return dispatch(
      process.get(),
      &ResourceProviderManagerProcess::api,
      request,
      principal);
}


void ResourceProviderManager::apply(
    const FrameworkID& frameworkId,
    const Offer::Operation& operation,
    const UUID& operationUUID)
{
  return dispatch(
      process.get(),
      &ResourceProviderManagerProcess::apply,
      frameworkId,
      operation,
      operationUUID);
}


Future<Nothing> ResourceProviderManager::publish(
    const SlaveID& slaveId,
    const Resources& resources)
{
  return dispatch(
      process.get(),
      &ResourceProviderManagerProcess::publish,
      slaveId,
      resources);
}


Queue<ResourceProviderMessage> ResourceProviderManager::messages() const
{
  return process->messages;
}

} // namespace internal {
} // namespace mesos {

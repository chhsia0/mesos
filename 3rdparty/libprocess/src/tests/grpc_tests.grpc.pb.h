// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: grpc_tests.proto
// Original file comments:
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
//
#ifndef GRPC_grpc_5ftests_2eproto__INCLUDED
#define GRPC_grpc_5ftests_2eproto__INCLUDED

#include "grpc_tests.pb.h"

#include <grpc++/impl/codegen/async_stream.h>
#include <grpc++/impl/codegen/async_unary_call.h>
#include <grpc++/impl/codegen/method_handler_impl.h>
#include <grpc++/impl/codegen/proto_utils.h>
#include <grpc++/impl/codegen/rpc_method.h>
#include <grpc++/impl/codegen/service_type.h>
#include <grpc++/impl/codegen/status.h>
#include <grpc++/impl/codegen/stub_options.h>
#include <grpc++/impl/codegen/sync_stream.h>

namespace grpc {
class CompletionQueue;
class Channel;
class RpcService;
class ServerCompletionQueue;
class ServerContext;
}  // namespace grpc

namespace tests {

class PingPong final {
 public:
  static constexpr char const* service_full_name() {
    return "tests.PingPong";
  }
  class StubInterface {
   public:
    virtual ~StubInterface() {}
    virtual ::grpc::Status Send(::grpc::ClientContext* context, const ::tests::Ping& request, ::tests::Pong* response) = 0;
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::tests::Pong>> AsyncSend(::grpc::ClientContext* context, const ::tests::Ping& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::tests::Pong>>(AsyncSendRaw(context, request, cq));
    }
  private:
    virtual ::grpc::ClientAsyncResponseReaderInterface< ::tests::Pong>* AsyncSendRaw(::grpc::ClientContext* context, const ::tests::Ping& request, ::grpc::CompletionQueue* cq) = 0;
  };
  class Stub final : public StubInterface {
   public:
    Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel);
    ::grpc::Status Send(::grpc::ClientContext* context, const ::tests::Ping& request, ::tests::Pong* response) override;
    std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::tests::Pong>> AsyncSend(::grpc::ClientContext* context, const ::tests::Ping& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::tests::Pong>>(AsyncSendRaw(context, request, cq));
    }

   private:
    std::shared_ptr< ::grpc::ChannelInterface> channel_;
    ::grpc::ClientAsyncResponseReader< ::tests::Pong>* AsyncSendRaw(::grpc::ClientContext* context, const ::tests::Ping& request, ::grpc::CompletionQueue* cq) override;
    const ::grpc::RpcMethod rpcmethod_Send_;
  };
  static std::unique_ptr<Stub> NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options = ::grpc::StubOptions());

  class Service : public ::grpc::Service {
   public:
    Service();
    virtual ~Service();
    virtual ::grpc::Status Send(::grpc::ServerContext* context, const ::tests::Ping* request, ::tests::Pong* response);
  };
  template <class BaseClass>
  class WithAsyncMethod_Send : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithAsyncMethod_Send() {
      ::grpc::Service::MarkMethodAsync(0);
    }
    ~WithAsyncMethod_Send() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status Send(::grpc::ServerContext* context, const ::tests::Ping* request, ::tests::Pong* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void RequestSend(::grpc::ServerContext* context, ::tests::Ping* request, ::grpc::ServerAsyncResponseWriter< ::tests::Pong>* response, ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) {
      ::grpc::Service::RequestAsyncUnary(0, context, request, response, new_call_cq, notification_cq, tag);
    }
  };
  typedef WithAsyncMethod_Send<Service > AsyncService;
  template <class BaseClass>
  class WithGenericMethod_Send : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithGenericMethod_Send() {
      ::grpc::Service::MarkMethodGeneric(0);
    }
    ~WithGenericMethod_Send() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status Send(::grpc::ServerContext* context, const ::tests::Ping* request, ::tests::Pong* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
  };
  template <class BaseClass>
  class WithStreamedUnaryMethod_Send : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithStreamedUnaryMethod_Send() {
      ::grpc::Service::MarkMethodStreamed(0,
        new ::grpc::StreamedUnaryHandler< ::tests::Ping, ::tests::Pong>(std::bind(&WithStreamedUnaryMethod_Send<BaseClass>::StreamedSend, this, std::placeholders::_1, std::placeholders::_2)));
    }
    ~WithStreamedUnaryMethod_Send() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable regular version of this method
    ::grpc::Status Send(::grpc::ServerContext* context, const ::tests::Ping* request, ::tests::Pong* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    // replace default version of method with streamed unary
    virtual ::grpc::Status StreamedSend(::grpc::ServerContext* context, ::grpc::ServerUnaryStreamer< ::tests::Ping,::tests::Pong>* server_unary_streamer) = 0;
  };
  typedef WithStreamedUnaryMethod_Send<Service > StreamedUnaryService;
  typedef Service SplitStreamedService;
  typedef WithStreamedUnaryMethod_Send<Service > StreamedService;
};

}  // namespace tests


#endif  // GRPC_grpc_5ftests_2eproto__INCLUDED

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

#include <string>

#include <gmock/gmock.h>

#include <process/future.hpp>
#include <process/grpc.hpp>
#include <process/gtest.hpp>

#include <stout/try.hpp>

#include "grpc_tests.pb.h"
#include "grpc_tests.grpc.pb.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;

using grpc::InsecureServerCredentials;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using process::Future;
using process::Promise;

using process::grpc::Channel;
using process::grpc::ClientRuntime;
using process::grpc::call;

using testing::_;
using testing::DoAll;
using testing::InvokeWithoutArgs;
using testing::Return;

namespace tests {

class PingPongServer : public PingPong::Service
{
public:
  PingPongServer()
  {
    EXPECT_CALL(*this, Send(_, _, _))
      .WillRepeatedly(Return(Status::OK));
  }

  MOCK_METHOD3(Send, Status(ServerContext*, const Ping* ping, Pong* pong));

  Try<Nothing> Startup(const string& addr)
  {
    ServerBuilder builder;
    builder.AddListeningPort(addr, InsecureServerCredentials());
    builder.RegisterService(this);

    server = builder.BuildAndStart();
    if (!server) {
      return Error("Unable to start a gRPC server.");
    }

    return Nothing();
  }

  Try<Nothing> Shutdown()
  {
    server->Shutdown();
    server->Wait();

    return Nothing();
  }

private:
  unique_ptr<grpc::Server> server;
};


// This test verifies that a gRPC future is properly set once the
// given call is processed by the server.
TEST(GRPCClientTest, Success)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;
  ASSERT_SOME(server.Startup(serverAddr));

  ClientRuntime runtime;
  Channel channel(serverAddr);

  Future<Pong> pong = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);

  AWAIT_EXPECT_READY(pong);

  ASSERT_SOME(server.Shutdown());
}


// This test verifies that a gRPC future fails if the server responds
// with a status other than OK for the given call.
TEST(GRPCTest, Failed)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(Return(Status::CANCELLED));

  ASSERT_SOME(server.Startup(serverAddr));

  ClientRuntime runtime;
  Channel channel(serverAddr);

  Future<Pong> pong = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);

  AWAIT_EXPECT_FAILED(pong);

  ASSERT_SOME(server.Shutdown());
}


// This test verifies that a gRPC future can be discarded before and
// when the server is processing the given call.
TEST(GRPCTest, Discarded)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;

  shared_ptr<Promise<Nothing>> processed(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> discarded(new Promise<Nothing>());

  // We're sending out two pings, but one will be cancelled before the
  // server starts up, so here we only expect one `Send` invocation.
  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          processed->set(Nothing());
          AWAIT_READY(discarded->future());
        }),
        Return(Status::OK)));

  ClientRuntime runtime;
  Channel channel(serverAddr);

  // Send and discard the 1st ping before the server starts.
  Future<Pong> pong1 = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);
  pong1.discard();

  ASSERT_SOME(server.Startup(serverAddr));

  // Send the 2nd ping then discard it when it is being processed.
  Future<Pong> pong2 = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);
  AWAIT_READY(processed->future());
  pong2.discard();
  discarded->set(Nothing());

  AWAIT_EXPECT_DISCARDED(pong1);
  AWAIT_EXPECT_DISCARDED(pong2);

  ASSERT_SOME(server.Shutdown());
}


// This test verifies that we can have multiple outstanding gRPC calls.
TEST(GRPCTest, Multiple)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;
  ASSERT_SOME(server.Startup(serverAddr));

  shared_ptr<Promise<Nothing>> processed1(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> processed2(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> processed3(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> pinged(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
      InvokeWithoutArgs([=] {
        processed1->set(Nothing());
        AWAIT_READY(pinged->future());
      }),
      Return(Status::OK)))
    .WillOnce(DoAll(
      InvokeWithoutArgs([=] {
        processed2->set(Nothing());
        AWAIT_READY(pinged->future());
      }),
      Return(Status::OK)))
    .WillOnce(DoAll(
      InvokeWithoutArgs([=] {
        processed3->set(Nothing());
        AWAIT_READY(pinged->future());
      }),
      Return(Status::OK)));

  ClientRuntime runtime;
  Channel channel(serverAddr);

  Future<Pong> pong1 = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);
  Future<Pong> pong2 = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);
  Future<Pong> pong3 = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);

  AWAIT_READY(processed1->future());
  AWAIT_READY(processed2->future());
  AWAIT_READY(processed3->future());

  pinged->set(Nothing());

  AWAIT_EXPECT_READY(pong1);
  AWAIT_EXPECT_READY(pong2);
  AWAIT_EXPECT_READY(pong3);

  ASSERT_SOME(server.Shutdown());
}


// This test verifies that a gRPC future fails properly when the runtime
// is shut down before the server responds.
TEST(GRPCTest, ClientShutdown)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;

  shared_ptr<Promise<Nothing>> processed(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> shutdown(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          processed->set(Nothing());
          AWAIT_READY(shutdown->future());
        }),
        Return(Status::OK)));

  ASSERT_SOME(server.Startup(serverAddr));

  shared_ptr<ClientRuntime> runtime(new ClientRuntime());
  Channel channel(serverAddr);

  Future<Pong> pong = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), *runtime);

  AWAIT_READY(processed->future());

  runtime.reset();
  shutdown->set(Nothing());

  AWAIT_EXPECT_FAILED(pong);

  ASSERT_SOME(server.Shutdown());
}

// This test verifies that a gRPC future fails if it is unable to
// connect to the server.
TEST(GRPCTest, ServerUnreachable)
{
  const string& serverAddr = "nosuchhost:50051";

  ClientRuntime runtime;
  Channel channel(serverAddr);

  Future<Pong> pong = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);

  AWAIT_EXPECT_FAILED(pong);
}


// This test verifies that a gRPC future fails properly when the server
// hangs when processing the given call.
TEST(GRPCTest, ServerTimeout)
{
  const string& serverAddr = "localhost:50051";

  PingPongServer server;

  shared_ptr<Promise<Nothing>> failed(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          AWAIT_READY(failed->future());
        }),
        Return(Status::OK)));

  ASSERT_SOME(server.Startup(serverAddr));

  ClientRuntime runtime;
  Channel channel(serverAddr);

  Future<Pong> pong = call(
      channel, GRPC_ASYNC_METHOD(PingPong, Send), Ping(), runtime);

  AWAIT_EXPECT_FAILED(pong);
  failed->set(Nothing());

  ASSERT_SOME(server.Shutdown());
}

} // namespace tests {

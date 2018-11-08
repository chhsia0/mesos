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

#include <chrono>
#include <string>

#include <gmock/gmock.h>

#include <process/future.hpp>
#include <process/grpc.hpp>
#include <process/gtest.hpp>

#include <stout/path.hpp>
#include <stout/try.hpp>

#include <stout/tests/utils.hpp>

#include "grpc_tests.grpc.pb.h"
#include "grpc_tests.pb.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;

using process::Future;
using process::Promise;

using process::grpc::CallOptions;
using process::grpc::Client;
using process::grpc::Connection;
using process::grpc::Server;
using process::grpc::StatusError;

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
      .WillRepeatedly(Return(::grpc::Status::OK));
  }

  MOCK_METHOD3(
      Send,
      ::grpc::Status(::grpc::ServerContext*, const Ping* ping, Pong* pong));

  Try<Connection> startup(const Option<string>& address = None())
  {
    ::grpc::ServerBuilder builder;

    if (address.isSome()) {
      builder.AddListeningPort(
          address.get(), ::grpc::InsecureServerCredentials());
    }

    builder.RegisterService(this);

    server = builder.BuildAndStart();
    if (!server) {
      return Error("Unable to start a gRPC server");
    }

    return address.isSome()
      ? Connection(address.get())
      : Connection(server->InProcessChannel(::grpc::ChannelArguments()));
  }

  Try<Nothing> shutdown()
  {
    server->Shutdown();
    server->Wait();

    return Nothing();
  }

private:
  unique_ptr<::grpc::Server> server;
};


class PingPongClient
{
public:
  PingPongClient(const Connection& connection)
    : stub(PingPong::NewStub(connection.channel)) {}

  Try<Pong, StatusError> send(const Ping& ping)
  {
    Pong pong;

    ::grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::milliseconds(100));

    ::grpc::Status status = stub->Send(&context, ping, &pong);

    if (!status.ok()) {
      return StatusError(status);
    }

    return pong;
  }

private:
  unique_ptr<PingPong::Stub> stub;
};


class GRPCClientTest : public TemporaryDirectoryTest
{
protected:
  // TODO(chhsiao): Consider removing this once we have a way to get a
  // connection before the server starts on Windows. See the
  // `DiscardedBeforeServerStarted` test below.
  string server_address() const
  {
    return "unix://" + path::join(sandbox.get(), "socket");
  }

  static CallOptions call_options()
  {
    CallOptions options;
    options.wait_for_ready = true;
    options.timeout = Milliseconds(100);

    return options;
  }
};


// This test verifies that a gRPC future is properly set once the
// given call is processed by the server.
TEST_F(GRPCClientTest, Success)
{
  PingPongServer server;
  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_ASSERT_READY(pong);
  EXPECT_SOME(pong.get());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that we can have multiple outstanding gRPC calls.
TEST_F(GRPCClientTest, ConcurrentRPCs)
{
  PingPongServer server;
  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

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
      Return(::grpc::Status::OK)))
    .WillOnce(DoAll(
      InvokeWithoutArgs([=] {
        processed2->set(Nothing());
        AWAIT_READY(pinged->future());
      }),
      Return(::grpc::Status::OK)))
    .WillOnce(DoAll(
      InvokeWithoutArgs([=] {
        processed3->set(Nothing());
        AWAIT_READY(pinged->future());
      }),
      Return(::grpc::Status::OK)));

  Client client;

  Future<Try<Pong, StatusError>> pong1 = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  Future<Try<Pong, StatusError>> pong2 = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  Future<Try<Pong, StatusError>> pong3 = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_READY(processed1->future());
  AWAIT_READY(processed2->future());
  AWAIT_READY(processed3->future());

  pinged->set(Nothing());

  AWAIT_ASSERT_READY(pong1);
  EXPECT_SOME(pong1.get());

  AWAIT_ASSERT_READY(pong2);
  EXPECT_SOME(pong2.get());

  AWAIT_ASSERT_READY(pong3);
  EXPECT_SOME(pong3.get());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that a gRPC future is set with an error when the server
// responds with a status other than OK for the given call.
TEST_F(GRPCClientTest, StatusError)
{
  PingPongServer server;

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(Return(::grpc::Status::CANCELLED));

  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_ASSERT_READY(pong);
  EXPECT_ERROR(pong.get());
  EXPECT_EQ(::grpc::CANCELLED, pong->error().status.error_code());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that a gRPC future can be discarded before the
// server processes the given call.
TEST_F_TEMP_DISABLED_ON_WINDOWS(GRPCClientTest, DiscardedBeforeServerStarted)
{
  PingPongServer server;

  EXPECT_CALL(server, Send(_, _, _))
    .Times(0);

  Connection connection(server_address());
  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection,
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  pong.discard();

  ASSERT_SOME(server.startup(server_address()));

  AWAIT_EXPECT_DISCARDED(pong);

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that a gRPC future can be discarded before and
// when the server is processing the given call.
TEST_F(GRPCClientTest, DiscardedWhenServerProcessing)
{
  PingPongServer server;

  shared_ptr<Promise<Nothing>> processed(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> discarded(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          processed->set(Nothing());
          AWAIT_READY(discarded->future());
        }),
        Return(::grpc::Status::OK)));

  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_READY(processed->future());

  pong.discard();
  discarded->set(Nothing());

  AWAIT_EXPECT_DISCARDED(pong);

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that a gRPC future is set with an error when the client
// is shut down before the server responds.
TEST_F(GRPCClientTest, ClientShutdown)
{
  PingPongServer server;

  shared_ptr<Promise<Nothing>> processed(new Promise<Nothing>());
  shared_ptr<Promise<Nothing>> shutdown(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          processed->set(Nothing());
          AWAIT_READY(shutdown->future());
        }),
        Return(::grpc::Status::OK)));

  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_READY(processed->future());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  shutdown->set(Nothing());

  AWAIT_ASSERT_READY(pong);
  EXPECT_ERROR(pong.get());
  EXPECT_EQ(::grpc::DEADLINE_EXCEEDED, pong->error().status.error_code());

  ASSERT_SOME(server.shutdown());
}


// This test verifies that a gRPC future is set with an error when it is unable
// to connect to the server.
TEST_F(GRPCClientTest, ServerUnreachable)
{
  Connection connection("nosuchhost");
  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection,
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  AWAIT_ASSERT_READY(pong);
  EXPECT_ERROR(pong.get());
  EXPECT_EQ(::grpc::DEADLINE_EXCEEDED, pong->error().status.error_code());
}


// This test verifies that a gRPC future is set with an error when the server
// hangs when processing the given call.
TEST_F(GRPCClientTest, ServerTimeout)
{
  PingPongServer server;

  shared_ptr<Promise<Nothing>> done(new Promise<Nothing>());

  EXPECT_CALL(server, Send(_, _, _))
    .WillOnce(DoAll(
        InvokeWithoutArgs([=] {
          AWAIT_READY(done->future());
        }),
        Return(::grpc::Status::OK)));

  Try<Connection> connection = server.startup();
  ASSERT_SOME(connection);

  Client client;

  Future<Try<Pong, StatusError>> pong = client.call(
      connection.get(),
      GRPC_CLIENT_RPC(PingPong, Send),
      Ping(),
      call_options());

  AWAIT_ASSERT_READY(pong);
  EXPECT_ERROR(pong.get());
  EXPECT_EQ(::grpc::DEADLINE_EXCEEDED, pong->error().status.error_code());

  done->set(Nothing());

  client.terminate();
  AWAIT_ASSERT_READY(client.wait());

  ASSERT_SOME(server.shutdown());
}


TEST(GRPCServerTest, Success)
{
  Server server;
  Future<Connection> connection = server.getInProcessConnection();

  server.route(
      GRPC_SERVER_RPC(PingPong, Send),
      [](const Ping&) { return Pong(); });

  server.run();

  AWAIT_READY(connection);

  PingPongClient client(connection.get());

  EXPECT_SOME(client.send(Ping()));
  EXPECT_SOME(client.send(Ping()));

  AWAIT_READY(server.stop());
}

} // namespace tests {

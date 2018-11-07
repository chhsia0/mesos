// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#ifndef __PROCESS_GRPC_HPP__
#define __PROCESS_GRPC_HPP__

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <google/protobuf/message.h>

#include <grpcpp/grpcpp.h>

#include <process/check.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/try.hpp>


// This file provides libprocess "support" for using gRPC. In particular, it
// defines two wrapper classes: `Connection` which represents a connection to a
// gRPC server, and `Client` which integrates an event loop waiting for gRPC
// responses and provides the `call` interface to create an asynchronous gRPC
// call and return a `Future`.


#define GRPC_CLIENT_RPC(service, rpc) \
  process::grpc::internal::make_rpc<service>(&service::Stub::PrepareAsync##rpc)

namespace process {
namespace grpc {

namespace internal {

typedef lambda::CallableOnce<void(bool)> CompletionCallback;


class ClientProcess : public Process<ClientProcess>
{
public:
  typedef lambda::CallableOnce<void(bool, ::grpc::CompletionQueue*)> CallFunc;

  ClientProcess();
  ~ClientProcess() override;

  void call(CallFunc func);

  void terminate();
  Future<Nothing> wait();

private:
  void initialize() override;
  void finalize() override;

  ::grpc::CompletionQueue completion_queue;
  std::unique_ptr<std::thread> looper;
  bool terminating = false;
  Promise<Nothing> terminated;
};


template <typename Service, typename Method>
struct RPC; // Undefined.


// Client unary RPC.
template <typename Service, typename Request, typename Response>
struct RPC<
    Service,
    std::unique_ptr<::grpc::ClientAsyncResponseReader<Response>>
      (Service::Stub::*)(
          ::grpc::ClientContext*,
          const Request&,
          ::grpc::CompletionQueue*)>
{
  typedef Service service_type;
  typedef Request request_type;
  typedef Response response_type;

  std::unique_ptr<::grpc::ClientAsyncResponseReader<Response>>
    (Service::Stub::*method)(
        ::grpc::ClientContext*,
        const Request&,
        ::grpc::CompletionQueue*);
};


template <typename Service, typename Method>
RPC<Service, Method> make_rpc(const Method& method)
{
  return RPC<Service, Method>{method};
}

} // namespace internal {


/**
 * Represents errors caused by non-OK gRPC statuses. See:
 * https://grpc.io/grpc/cpp/classgrpc_1_1_status.html
 */
class StatusError : public Error
{
public:
  StatusError(::grpc::Status _status)
    : Error(_status.error_message()), status(std::move(_status))
  {
    CHECK(!status.ok());
  }

  const ::grpc::Status status;
};


/**
 * A copyable interface to manage a connection to a gRPC server. All
 * `Connection` copies share the same gRPC channel which is thread safe. Note
 * that the actual connection is established lazily by the gRPC library at the
 * time an RPC is made to the channel.
 */
class Connection
{
public:
  Connection(
      const std::string& uri,
      const std::shared_ptr<::grpc::ChannelCredentials>& credentials =
        ::grpc::InsecureChannelCredentials())
    : channel(::grpc::CreateChannel(uri, credentials)) {}

  explicit Connection(std::shared_ptr<::grpc::Channel> _channel)
    : channel(std::move(_channel)) {}

  const std::shared_ptr<::grpc::Channel> channel;
};


/**
 * Defines the gRPC options for each call.
 */
struct CallOptions
{
  // Enable the gRPC wait-for-ready semantics by default so the call will be
  // retried if the connection is not ready. See:
  // https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
  bool wait_for_ready = true;

  // The timeout of the call. A `DEADLINE_EXCEEDED` status will be returned if
  // there is no response in the specified amount of time. This is required to
  // avoid the call from being pending forever.
  Duration timeout = Seconds(60);
};


/**
 * A copyable interface to manage an internal client process for serializing
 * requests, making asynchronous gRPC calls, and deserializing responses. The
 * client process also manages an extra looper thread to wait for incoming
 * responses from the gRPC `CompletionQueue`. All `Client` copies share the same
 * client process, and a client process can be used to make gRPC calls through
 * different connections. Usually we only need a single client process to handle
 * all gRPC calls, but multiple client processes can be instantiated for better
 * parallelism and isolation.
 *
 * NOTE: The caller must call `terminate` to drain the `CompletionQueue` before
 * finalizing libprocess to gracefully terminate the gRPC client.
 */
class Client
{
public:
  Client() : data(new Data()) {}

  /**
   * Sends an asynchronous gRPC call.
   *
   * This function returns a `Future` of a `Try` such that the response protobuf
   * is returned only if the gRPC call returns an OK status to ensure type
   * safety (see https://github.com/grpc/grpc/issues/12824). Note that the
   * future never fails; it will return a `StatusError` if a non-OK status is
   * returned for the call, so the caller can handle the error programmatically.
   *
   * @param connection A connection to a gRPC server.
   * @param method The asynchronous gRPC call to make. This should be obtained
   *     by the `GRPC_CLIENT_RPC(service, rpc)` macro.
   * @param request The request protobuf for the gRPC call.
   * @param options The gRPC options for the call.
   * @return a `Future` of `Try` waiting for a response protobuf or an error.
   */
  template <
      typename RPC,
      typename Request,
      typename std::enable_if<
          std::is_same<
              typename std::decay<Request>::type,
              typename RPC::request_type>::value,
          int>::type = 0>
  Future<Try<typename RPC::response_type, StatusError>> call(
      const Connection& connection,
      RPC&& rpc,
      Request&& request,
      const CallOptions& options)
  {
    using Response = typename RPC::response_type;

    // Create a `Promise` that will be set upon receiving a response.
    //
    // TODO(chhsiao): The `Promise` in the `shared_ptr` is not shared, but only
    // to be captured by the lambda below. Use a `unique_ptr` once we get C++14.
    auto promise = std::make_shared<Promise<Try<Response, StatusError>>>();
    Future<Try<Response, StatusError>> future = promise->future();

    // Send the request in the internal client process.
    //
    // TODO(chhsiao): We use `lambda::partial` here to forward `request` to
    // avoid an extra copy. Capture it by forwarding once we get C++14.
    dispatch(data->pid, &internal::ClientProcess::call, lambda::partial(
        [connection, rpc, options, promise](
            Request&& request,
            bool running,
            ::grpc::CompletionQueue* completion_queue) {
          if (!running) {
            promise->fail("Client has been terminated");
            return;
          }

          // TODO(chhsiao): The `shared_ptr`s here aren't shared, but only to be
          // captured by the lambda below. Use `unique_ptr`s once we get C++14.
          auto context = std::make_shared<::grpc::ClientContext>();
          auto response = std::make_shared<Response>();
          auto status = std::make_shared<::grpc::Status>();

          context->set_wait_for_ready(options.wait_for_ready);

          // NOTE: We need to ensure that we're using a
          // `std::chrono::system_clock::time_point` because `grpc::TimePoint`
          // provides a specialization only for this type and we cannot
          // guarantee that the operation below will always result in this type.
          context->set_deadline(
              std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                  std::chrono::system_clock::now() +
                  std::chrono::nanoseconds(options.timeout.ns())));

          promise->future().onDiscard([=] { context->TryCancel(); });

          typename RPC::service_type::Stub stub(connection.channel);
          std::shared_ptr<::grpc::ClientAsyncResponseReader<Response>> reader =
            (stub.*(rpc.method))(context.get(), request, completion_queue);

          reader->StartCall();

          // Create a `CompletionCallback` as a tag in the `CompletionQueue` for
          // the current asynchronous gRPC call. The callback will set up the
          // above `Promise` upon receiving a response.
          //
          // NOTE: `context` and `reader` need to be held on in order to get
          // updates for the ongoing RPC, and thus are captured here. The
          // callback itself will later be retrieved in the looper thread.
          void* tag = new internal::CompletionCallback(
              [context, reader, response, status, promise](bool ok) {
                // `ok` should always be true for unary RPCs. See:
                // https://grpc.io/grpc/cpp/classgrpc_1_1_completion_queue.html#a86d9810ced694e50f7987ac90b9f8c1a // NOLINT
                CHECK(ok);

                CHECK_PENDING(promise->future());
                if (promise->future().hasDiscard()) {
                  promise->discard();
                } else {
                  promise->set(status->ok()
                    ? std::move(*response)
                    : Try<Response, StatusError>::error(std::move(*status)));
                }
              });

          reader->Finish(response.get(), status.get(), tag);
        },
        std::forward<Request>(request),
        lambda::_1,
        lambda::_2));

    return future;
  }

  /**
   * Asks the internal client process to shut down the `CompletionQueue`, which
   * would asynchronously drain and fail all pending gRPC calls in the
   * `CompletionQueue`, then join the looper thread.
   */
  void terminate();

  /**
   * @return A `Future` waiting for all pending gRPC calls in the
   *     `CompletionQueue` of the internal client process to be drained and the
   *     looper thread to be joined.
   */
  Future<Nothing> wait();

private:
  struct Data
  {
     Data();
     ~Data();

     PID<internal::ClientProcess> pid;
     Future<Nothing> terminated;
  };

  std::shared_ptr<Data> data;
};

} // namespace grpc {
} // namespace process {

#endif // __PROCESS_GRPC_HPP__

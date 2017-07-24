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
#include <thread>

#include <grpc/support/time.h>

#include <grpc++/grpc++.h>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/lambda.hpp>
#include <stout/try.hpp>


// This file provides libprocess "support" for using gRPC. In
// particular, it defines two wrapper classes: `Channel` (representing a
// connection to a gRPC server) and `ClientRuntime` (integrating an
// event loop waiting for gRPC responses), and the `call` interface to
// create an asynchrous gRPC call and wrap it as a `Future`.


#define GRPC_ASYNC_METHOD(service, method) \
  (&service::Stub::Async##method)

namespace process {
namespace grpc {

// Forward declarations.
namespace internal { struct Call; }


class ClientRuntime
{
public:
  ClientRuntime() : looper(new Looper) {}

private:
  struct Looper
  {
    Looper();
    ~Looper();

    void loop();

    std::unique_ptr<std::thread> thread;
    ::grpc::CompletionQueue queue;
    ProcessBase process;
  };

  std::shared_ptr<Looper> looper;

  friend struct internal::Call;
};


class Channel
{
public:
  Channel(const std::string& uri,
          const std::shared_ptr<::grpc::ChannelCredentials>& credentials =
            ::grpc::InsecureChannelCredentials())
    : channel(::grpc::CreateChannel(uri, credentials)) {}

private:
  std::shared_ptr<::grpc::Channel> channel;

  friend struct internal::Call;
};


namespace internal {

// NOTE: This struct is used by the public `call()` function. It hides
// the type signature of a gRPC asynchronous method, and make it easier
// for friend declaration.
struct Call
{
  // Creates a promise and a callback lambda as a tag and invokes an
  // asynchronous gRPC call through the `CompletionQueue` associated
  // with the `runtime`, then returns the future of the promise.
  template <typename T, typename Request, typename Response>
  Future<Response> operator()(
    const Channel& channel,
    std::unique_ptr<::grpc::ClientAsyncResponseReader<Response>>(T::*method)(
      ::grpc::ClientContext*, const Request&, ::grpc::CompletionQueue*),
    const Request& request,
    const ClientRuntime& runtime)
  {
    if (!runtime.looper) {
      return Failure("Runtime has not been initialized yet.");
    }

    std::shared_ptr<::grpc::ClientContext> context(new ::grpc::ClientContext());
    context->set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(5));

    std::shared_ptr<Promise<Response>> promise(new Promise<Response>);
    promise->future().onDiscard([=] { context->TryCancel(); });

    std::shared_ptr<Response> response(new Response());
    std::shared_ptr<::grpc::Status> status(new ::grpc::Status());

    std::shared_ptr<::grpc::ClientAsyncResponseReader<Response>> reader =
      std::move((T(channel.channel).*method)(
          context.get(), request, &runtime.looper->queue));
    reader->Finish(response.get(), status.get(), new lambda::function<void()>(
        [context, reader, response, status, promise]() {
          CHECK(promise->future().isPending());
          if (promise->future().hasDiscard()) {
            promise->discard();
          } else if (status->ok()) {
            promise->set(*response);
          } else {
            promise->fail(status->error_message());
          }
        }));


    return promise->future();
  }
};

} // namespace internal {


// The public `call()` function makes an asynchronous gRPC call whose
// response is handled by the specified ClientProcess upon receipt.
template <typename Method, typename Request>
inline auto call(
    const Channel& channel,
    const Method& method,
    const Request& request,
    ClientRuntime& runtime)
  -> decltype(internal::Call()(channel, method, request, runtime))
{
  return internal::Call()(channel, method, request, runtime);
}

} // namespace grpc {
} // namespace process {

#endif // __PROCESS_GRPC_HPP__

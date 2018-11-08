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

#include <process/grpc.hpp>

#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace process {
namespace grpc {

namespace internal {

static void completion_loop(
    const UPID& pid, ::grpc::CompletionQueue* completion_queue)
{
  void* tag;
  bool ok = false;

  while (completion_queue->Next(&tag, &ok)) {
    // Obtain the tag as a `CompletionCallback` and dispatch it to the client
    // process. The tag is then reclaimed in the process.
    CompletionCallback* callback = reinterpret_cast<CompletionCallback*>(tag);
    dispatch(pid, [=]{ std::move(*callback)(ok); delete callback; });
  }

  // Terminate self after all events are drained.
  process::terminate(pid, false);
}


ClientProcess::ClientProcess()
  : ProcessBase(ID::generate("__grpc_client__")) {}


ClientProcess::~ClientProcess()
{
  CHECK(!looper);
}


void ClientProcess::call(CallFunc func)
{
  std::move(func)(!terminating, &completion_queue);
}


void ClientProcess::terminate()
{
  if (!terminating) {
    terminating = true;
    completion_queue.Shutdown();
  }
}


Future<Nothing> ClientProcess::wait()
{
  return terminated.future();
}


void ClientProcess::initialize()
{
  // The looper thread can only be created here since it need to happen
  // after `completion_queue` is initialized.
  looper.reset(new std::thread(&completion_loop, self(), &completion_queue));
}


void ClientProcess::finalize()
{
  CHECK(terminating) << "Client has not yet been terminated";

  // NOTE: This is a blocking call. However, the thread is guaranteed
  // to be exiting, therefore the amount of blocking time should be
  // short (just like other syscalls we invoke).
  looper->join();
  looper.reset();
  terminated.set(Nothing());
}


ServerProcess::ServerProcess(
    const string& _uri,
    const shared_ptr<::grpc::ServerCredentials>& _credentials)
  : ProcessBase(ID::generate("__grpc_server__")),
    uri(_uri),
    credentials(_credentials),
    state(State::INITIALIZED) {}


ServerProcess::ServerProcess()
  : ProcessBase(ID::generate("__grpc_server__")),
    state(State::INITIALIZED) {}


ServerProcess::~ServerProcess()
{
  CHECK(!looper);
}


void ServerProcess::route(::grpc::Service* service, RouteFunc func)
{
  services.emplace_back(service);
  handlers.emplace_back(std::move(func));
}


Future<Nothing> ServerProcess::run()
{
  ::grpc::ServerBuilder builder;

  if (uri.isSome()) {
    builder.AddListeningPort(uri.get(), credentials);
  }

  foreach (::grpc::Service* service, services) {
    builder.RegisterService(service);
  }

  completion_queue = builder.AddCompletionQueue();
  server = builder.BuildAndStart();

  foreach (RouteFunc& func, handlers) {
    std::move(func)(true, &services, completion_queue.get());
  }

  // The looper thread can only be created here since it need to happen
  // after `completion_queue` is initialized.
  looper.reset(
      new std::thread(&completion_loop, self(), completion_queue.get()));

  return state.transition<State::INITIALIZED, State::RUNNING>([this] {
    return state.when<State::STOPPED>();
  });
}


Future<Nothing> ServerProcess::stop()
{
  return state.transition<State::RUNNING, State::STOPPING>([this] {
    server->Shutdown();
    completion_queue->Shutdown();

    return state.when<State::STOPPED>();
  },
  "Server must be started in order to be stopped");
}


Future<Connection> ServerProcess::getInProcessConnection()
{
  return state.when<State::RUNNING>()
    .then(defer(self(), [this] {
      CHECK(server);
      return Connection(server->InProcessChannel(::grpc::ChannelArguments()));
    }));
}


void ServerProcess::finalize()
{
  Try<Nothing> stopped = state.transition<State::STOPPING, State::STOPPED>();
  CHECK_SOME(stopped) << "Server has not yet been stopped";

  // NOTE: This is a blocking call. However, the thread is guaranteed
  // to be exiting, therefore the amount of blocking time should be
  // short (just like other syscalls we invoke).
  looper->join();
  looper.reset();
}

} // namespace internal {


void Client::terminate()
{
  dispatch(data->pid, &internal::ClientProcess::terminate);
}


Future<Nothing> Client::wait()
{
  return data->terminated;
}


Client::Data::Data()
{
  internal::ClientProcess* process = new internal::ClientProcess();
  terminated = process->wait();
  pid = spawn(process, true);
}


Client::Data::~Data()
{
  dispatch(pid, &internal::ClientProcess::terminate);
}


Server::Server(
    const string& uri,
    const shared_ptr<::grpc::ServerCredentials>& credentials)
  : pid(spawn(new internal::ServerProcess(uri, credentials), true)) {}


Server::Server()
  : pid(spawn(new internal::ServerProcess(), true)) {}


Future<Nothing> Server::run()
{
  return dispatch(pid, &internal::ServerProcess::run);
}


Future<Nothing> Server::stop()
{
  return dispatch(pid, &internal::ServerProcess::stop);
}


Future<Connection> Server::getInProcessConnection()
{
  return dispatch(pid, &internal::ServerProcess::getInProcessConnection);
}

} // namespace grpc {
} // namespace process {

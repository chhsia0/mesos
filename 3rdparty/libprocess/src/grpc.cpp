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

#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/process.hpp>

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

} // namespace grpc {
} // namespace process {

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

#include <process/dispatch.hpp>
#include <process/grpc.hpp>
#include <process/id.hpp>

namespace process {
namespace grpc {


ClientRuntime::Looper::Looper()
  : process(ID::generate("__grpc_client__"))
{
  spawn(process);
  // The looper thread can only be created here since it need to happen
  // after `queue` is initialized.
  thread.reset(new std::thread(&ClientRuntime::Looper::loop, this));
}


ClientRuntime::Looper::~Looper()
{
  queue.Shutdown();
  thread->join();
  terminate(process);
  wait(process);
}


void ClientRuntime::Looper::loop()
{
  void* tag;
  bool ok;

  while (queue.Next(&tag, &ok)) {
    // The returned callback object is managed by the `callback` shared
    // pointer, so if we get a regular event from the `CompletionQueue`,
    // then the object would be captured by the following lambda
    // dispatched to `process`; otherwise it would be reclaimed here.
    std::shared_ptr<lambda::function<void()>> callback(
        reinterpret_cast<lambda::function<void()>*>(tag));
    if (ok) {
      dispatch(process, [=] { (*callback)(); });
    }
  }
}

} // namespace grpc {
} // namespace process {

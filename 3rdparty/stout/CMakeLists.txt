# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(StoutConfigure)
include(GroupSource)

# SOURCE GROUPS. Allows IDEs to group header files for projects taking a
# dependency on this package.
########################################################################
file(
  GLOB_RECURSE
  STOUT_HEADERS
  "${CMAKE_CURRENT_SOURCE_DIR}/include/stout/*.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/stout/*.h"
  )

macro(GROUP_STOUT_HEADERS)
  GROUP_SOURCE(
    "Stout Public Headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/stout"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/stout"
    "*.h*")
endmacro()

# TODO(andschwa): Remove this when all libraries are fully qualified.
link_directories(${STOUT_LIB_DIRS})

# THE STOUT LIBRARY INTERFACE
#############################
add_library(stout INTERFACE)

# TODO(andschwa): Remove this when all dependencies have top-level targets.
add_dependencies(stout ${STOUT_DEPENDENCIES})

target_include_directories(stout INTERFACE
  include/
  ${STOUT_3RDPARTY_INCLUDE_DIRS})

target_link_libraries(stout INTERFACE
  ${STOUT_LIBS})

GROUP_STOUT_HEADERS()

add_subdirectory(tests)

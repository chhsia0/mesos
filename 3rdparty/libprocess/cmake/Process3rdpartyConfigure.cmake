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

# DEFINE DIRECTORY STRUCTURE FOR THIRD-PARTY LIBS.
##################################################
EXTERNAL("boost"           ${BOOST_VERSION}           "${MESOS_3RDPARTY_BIN}")
EXTERNAL("concurrentqueue" ${CONCURRENTQUEUE_VERSION} "${MESOS_3RDPARTY_BIN}")
EXTERNAL("elfio"           ${ELFIO_VERSION}           "${MESOS_3RDPARTY_BIN}")
EXTERNAL("picojson"        ${PICOJSON_VERSION}        "${MESOS_3RDPARTY_BIN}")
EXTERNAL("http_parser"     ${HTTP_PARSER_VERSION}     "${MESOS_3RDPARTY_BIN}")
EXTERNAL("libev"           ${LIBEV_VERSION}           "${MESOS_3RDPARTY_BIN}")
EXTERNAL("libevent"        ${LIBEVENT_VERSION}        "${MESOS_3RDPARTY_BIN}")
EXTERNAL("libapr"          ${LIBAPR_VERSION}          "${MESOS_3RDPARTY_BIN}")
EXTERNAL("nvml"            ${NVML_VERSION}            "${MESOS_3RDPARTY_BIN}")

if (WIN32)
  # NOTE: We expect cURL and zlib exist on Unix (usually pulled in with a
  # package manager), but Windows has no package manager, so we have to go
  # get it.
  EXTERNAL("zlib" ${ZLIB_VERSION} "${MESOS_3RDPARTY_BIN}")
endif ()

# Intermediate convenience variables for oddly-structured directories.
set(LIBEV_LIB_ROOT    ${LIBEV_ROOT}-lib/lib)
set(LIBEVENT_LIB_ROOT ${LIBEVENT_ROOT}-lib/lib)

# Convenience variables for include directories of third-party dependencies.
set(PROCESS_INCLUDE_DIR ${MESOS_3RDPARTY_SRC}/libprocess/include)

set(BOOST_INCLUDE_DIR           ${BOOST_ROOT})
set(CONCURRENTQUEUE_INCLUDE_DIR ${CONCURRENTQUEUE_ROOT})
set(ELFIO_INCLUDE_DIR           ${ELFIO_ROOT})
set(GPERFTOOLS_INCLUDE_DIR      ${GPERFTOOLS}/src)
set(HTTP_PARSER_INCLUDE_DIR     ${HTTP_PARSER_ROOT})
set(LIBEV_INCLUDE_DIR           ${LIBEV_ROOT})
set(NVML_INCLUDE_DIR            ${NVML_ROOT})
set(PICOJSON_INCLUDE_DIR        ${PICOJSON_ROOT})

if (WIN32)
  set(APR_INCLUDE_DIR      ${LIBAPR_ROOT}/include ${LIBAPR_ROOT}-build)
  set(LIBEVENT_INCLUDE_DIR
    ${LIBEVENT_ROOT}/include
    ${LIBEVENT_ROOT}-build/include)
  set(ZLIB_INCLUDE_DIR     ${ZLIB_ROOT} ${ZLIB_ROOT}-build)
else ()
  set(LIBEVENT_INCLUDE_DIR ${LIBEVENT_LIB_ROOT}/include)
endif ()

# Convenience variables for `lib` directories of built third-party dependencies.
set(LIBEV_LIB_DIR       ${LIBEV_ROOT}-build/.libs)

if (WIN32)
  set(HTTP_PARSER_LIB_DIR ${HTTP_PARSER_ROOT}-build)
  set(LIBEVENT_LIB_DIR    ${LIBEVENT_ROOT}-build/lib)
  set(ZLIB_LIB_DIR        ${ZLIB_ROOT}-build)
else ()
  set(HTTP_PARSER_LIB_DIR ${HTTP_PARSER_ROOT}-build)
  set(LIBEVENT_LIB_DIR    ${LIBEVENT_LIB_ROOT}/lib)
endif ()

# Convenience variables for "lflags", the symbols we pass to CMake to generate
# things like `-L/path/to/glog` or `-lglog`.
set(HTTP_PARSER_LFLAG http_parser)
set(LIBEV_LFLAG       ev)
set(LIBEVENT_LFLAG    event)

if (WIN32)

  # Zlib generates different libraries depending on the linkage
  # and configuration.  i.e.:
  #   * For a static Debug build: `zlibstaticd`.
  #   * For a shared Release build: `zlib`.
  set(ZLIB_LFLAG zlib)

  if (NOT BUILD_SHARED_LIBS)
    string(APPEND ZLIB_LFLAG static)
  endif ()

  string(APPEND ZLIB_LFLAG $<$<CONFIG:Debug>:d>)
else ()
  set(DL_LFLAG       dl)
  set(SASL_LFLAG     sasl2)
endif ()

# Configure the process library, the last of our third-party libraries.
#######################################################################
include(ProcessConfigure)

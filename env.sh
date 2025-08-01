# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# check STARROCKS_HOME
if [[ -z ${STARROCKS_HOME} ]]; then
    echo "Error: STARROCKS_HOME is not set"
    exit 1
fi

# include custom environment variables
if [[ -f ${STARROCKS_HOME}/custom_env.sh ]]; then
    . ${STARROCKS_HOME}/custom_env.sh
fi

# set STARROCKS_THIRDPARTY
if [[ -z ${STARROCKS_THIRDPARTY} ]]; then
    export STARROCKS_THIRDPARTY=${STARROCKS_HOME}/thirdparty
fi

# check python
if [[ -z ${PYTHON} ]]; then
    export PYTHON=python3
fi

if ${PYTHON} --version | grep -q '^Python 3\.'; then
    echo "Found python3, version: `\${PYTHON} --version`"
else
    echo "Error: python3 is needed"
    exit 1
fi


# set GCC HOME
if [[ -z ${STARROCKS_GCC_HOME} ]]; then
    export STARROCKS_GCC_HOME=$(dirname `which gcc`)/..
fi

gcc_ver=`${STARROCKS_GCC_HOME}/bin/gcc -dumpfullversion -dumpversion`
required_ver="12.1.0"
if [[ ! "$(printf '%s\n' "$required_ver" "$gcc_ver" | sort -V | head -n1)" = "$required_ver" ]]; then
    echo "Error: GCC version (${gcc_ver}) must be greater than or equal to ${required_ver}"
    exit 1
fi

# export CLANG COMPATIBLE FLAGS
export CLANG_COMPATIBLE_FLAGS=`echo | ${STARROCKS_GCC_HOME}/bin/gcc -Wp,-v -xc++ - -fsyntax-only 2>&1 \
                | grep -E '^\s+/' | awk '{print "-I" $1}' | tr '\n' ' '`

if [[ -z ${JAVA_HOME} ]]; then
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
    echo "Infered JAVA_HOME=$JAVA_HOME"
fi

if [[ -z ${JAVA_HOME} ]]; then
    echo "Error: JAVA_HOME is not set"
    exit 1
fi

if ! command -v $JAVA_HOME/bin/java &> /dev/null; then
    echo "Error: JAVA not found, JAVA_HOME may be set wrong"
    exit 1
fi

# check java version
export JAVA=${JAVA_HOME}/bin/java
# Some examples of different variant of jdk output for `java -version`
# - Oracle JDK: java version "1.8.0_202"
# - OpenJDK: openjdk version "1.8.0_362"
# - OpenJDK: openjdk version "11.0.20.1" 2023-08-24
JAVA_VER=$(${JAVA} -version 2>&1 | awk -F'"' '{print $2}' | awk -F. '{if ($1 == 1) {print $2;} else {print $1;}}')
if [[ $JAVA_VER -lt 11 ]]; then
    echo "Error: require JAVA with JDK version at least 11, but got $JAVA_VER"
    exit 1
fi

# check maven
MVN_CMD=mvn
if [[ ! -z ${CUSTOM_MVN} ]]; then
    MVN_CMD=${CUSTOM_MVN}
fi
if ! ${MVN_CMD} --version; then
    echo "Error: mvn is not found"
    exit 1
fi
export MVN_CMD

CMAKE_CMD=cmake
if [[ ! -z ${CUSTOM_CMAKE} ]]; then
    CMAKE_CMD=${CUSTOM_CMAKE}
fi
export CMAKE_CMD

CMAKE_GENERATOR="Unix Makefiles"
BUILD_SYSTEM="make"
if ninja --version 2>/dev/null; then
    BUILD_SYSTEM="ninja"
    CMAKE_GENERATOR="Ninja"
fi
export CMAKE_GENERATOR
export BUILD_SYSTEM

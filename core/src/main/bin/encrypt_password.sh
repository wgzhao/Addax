#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# encrypt the giving password

SCRIPT_PATH="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

ADDAX_HOME=$(dirname $SCRIPT_PATH)
if [ -z "${ADDAX_HOME}" ]; then
    exit 2
fi

if [ $# -ne 1 ]; then
  echo "Usage: $0 <password>"
  exit 1
fi

cd ${ADDAX_HOME}
commjar=$(ls lib/addax-common-*.jar lib/slf4j-*.jar lib/logback*.jar |tr '\t' ':')
for jar in ${commjar[@]}
do
   classpath=${classpath}:$jar
done
java -cp $classpath com.wgzhao.addax.common.util.EncryptUtil $1
#java -cp ${ADDAX_HOME}/lib/addax-common-*.jar:${ADDAX_HOME}/lib/slf4j-api-*.jar com.wgzhao.addax.core.util.EncryptUtil $1

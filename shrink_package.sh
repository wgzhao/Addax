#!/bin/bash
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing,
#  * software distributed under the License is distributed on an
#  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  * KIND, either express or implied.  See the License for the
#  * specific language governing permissions and limitations
#  * under the License.
#  */
#
function get_version() {
    version=$(grep -o -E '<version>.*</version>' pom.xml | head -n 1 | sed -e 's/<version>\(.*\)<\/version>/\1/')
    echo $version
}
echo "Begin shrinking package..."
target="$(dirname $0)/target"
[ -d ${target} ] || exit 1
version=$(get_version)
[ -d ${target}/addax-${version} ] || exit 2


cd ${target}/addax-${version} || exit 3
# should be in target/addax-<version>
[ -d shared ] || mkdir shared

for jar in $(find  plugin/*/*/libs -type f -name *.jar)
do
    plugin_dir=$(dirname $jar)
    file_name=$(basename $jar)
    # 1. move it to shared folder
    /bin/mv -f ${jar} shared/
    # 2. create symbol link
    ( cd ${plugin_dir} && ln -sf ../../../../shared/${file_name} $file_name )
done

cd -

if [ "x$1" = "xy" ]; then
  # create archive package
  cd ${target}
  echo "Create archived package"
  tar -czf "addax-${version}.tar.gz" addax-${version}/
  echo "The archive package is at: ${target}/addax-${version}.tar.gz"
fi

exit $?


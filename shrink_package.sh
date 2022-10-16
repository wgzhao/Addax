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
echo "Begin shrinking package..."
target="$(dirname $0)/target"
[ -d ${target} ] || exit 1
TMPDIR=$(ls -d -w1 target/addax/addax-*)
[ -n "$TMPDIR" ] || exit 2

cd ${TMPDIR} || exit 3
# should be in target/addax/addax-<version>
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

# create archive package
cd target/addax
# should be in target/addax/
# get archive name including version
archive_name=$(basename $TMPDIR)
echo "Create archived package"
tar -czf "${archive_name}.tar.gz" ${archive_name}/*
echo "The archive package is at: ${target}/addax/${archive_name}.tar.gz"
exit $?


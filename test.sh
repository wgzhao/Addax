#!/bin/bash
# 基本测试 
version=$(head -n10 pom.xml|grep '<version>' |tr -d '<version>/ ') 
cd target/datax-${version}/datax-${version}
echo "DataX Version: ${version}"
echo "Test stream to output......."
/opt/anaconda3/bin/python3 bin/datax.py job/job.json
if [ $? -ne 0 ];then
  exit 1
fi
echo "Test stream to text......"
/opt/anaconda3/bin/python3 bin/datax.py job/stream2txt.json
if [$? -ne 0 ];then
  exit 2
fi
exit 0

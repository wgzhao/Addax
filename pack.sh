#!/bin/bash
VER=$(grep -A1 '<artifactId>datax-all</artifactId>' ./pom.xml |tail -n1 |tr -d '<version/> ')
[ -d target ] && rm -rf target
mkdir -p target/datax

# core
echo "processing core...."
cp -a core/target/datax/* target/datax/
cp -a core/target/datax-core-${VER}.jar target/datax/lib/

echo "processing reader plugins...."
# reader plugins
for item in $(ls -dw1 *reader)
do
    if [ -f ${item}/target/${item}-${VER}.jar ];then
        mkdir -p target/datax/plugin/reader/${item}
        cp -a ${item}/target/${item}-${VER}.jar target/datax/plugin/reader/${item}/
        rsync -az  ${item}/target/datax/plugin/reader/${item}/* target/datax/plugin/reader/${item}/
    fi
done

#writer plugins
echo "processing writer plugins...."
for item in $(ls -dw1 *writer)
do
    if [ -f ${item}/target/${item}-${VER}.jar ];then
        mkdir -p target/datax/plugin/writer/${item}
        cp -a ${item}/target/${item}-${VER}.jar target/datax/plugin/writer/${item}/
        rsync -az  ${item}/target/datax/plugin/writer/${item}/* target/datax/plugin/writer/${item}/
    fi
done

#delete duplicate jars
echo "deleting duplicate jars"
cd target/datax/lib
for i in *.jar
do
find ../plugin/ -name ${i} -exec rm -f '{}' \;
done
cd ../../../

#packing
echo "packing....."
cd target
#tar -czf datax-${VER}.tar.gz datax
exit 0

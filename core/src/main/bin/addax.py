#!/usr/bin/env python
from __future__ import print_function

import codecs
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
from glob import glob
from optparse import OptionGroup
from optparse import OptionParser
from string import Template


def isWindows():
    return platform.system() == 'Windows'


def get_version():
    """
    extract version from lib/addax-core-<version>.jar package
    """
    core_jar = glob(os.path.join(ADDAX_HOME, "lib", "addax-core-*.jar"))
    if not core_jar:
        return ""
    else:
        return os.path.basename(core_jar[0])[11:-4]


ADDAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ADDAX_VERSION = 'Addax ' + get_version()
CODING = '-Dfile.encoding=UTF-8'
if isWindows():
    codecs.register(
        lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    CLASS_PATH = "{1}{0}lib{0}*".format(os.sep, ADDAX_HOME)
    CODING = '-Dfile.encoding=cp936'
else:
    CLASS_PATH = ".:/etc/hbase/conf:{1}{0}lib{0}*".format(os.sep, ADDAX_HOME)

LOGBACK_FILE = "{1}{0}conf{0}logback.xml".format(os.sep, ADDAX_HOME)
DEFAULT_JVM = "-Xms64m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath={}".format(
    ADDAX_HOME)
DEFAULT_PROPERTY_CONF = "%s -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
                        -Djava.security.egd=file:///dev/urandom -Daddax.home=%s -Dlogback.configurationFile=%s " % \
                        (CODING, ADDAX_HOME, LOGBACK_FILE)
ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.wgzhao.addax.core.Engine -job ${job}" % \
                 (DEFAULT_PROPERTY_CONF, CLASS_PATH)
REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"

RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    "OK": 0,
    "RUN": 1,
    "RETRY": 2
}


def suicide(signum, e):
    global child_process
    print("[Error] Addax receive unexpected signal {}, starts to suicide.".format(signum))

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    print("Addax Process was killed ! you did ?")
    sys.exit(RET_STATE["KILL"])


def register_signal():
    if not isWindows():
        global child_process
        signal.signal(2, suicide)
        signal.signal(3, suicide)
        signal.signal(15, suicide)


def getOptionParser():
    usage = "usage: %prog [options] job-url-or-path"
    parser = OptionParser(usage=usage)
    parser.add_option("-v", "--version", action="store_true",
                      help="Print version and exit")

    parser.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters", action="store",
                                  help="Set jvm parameters if necessary.")
    parser.add_option("-p", "--params", metavar="<parameter used in job config>",
                                  action="store", dest="params",
                                  help='Set job parameter, eg: the source tableName you want to set it by command, '
                                       'then you can use like this: -p"-DtableName=your-table-name", '
                                       'if you have multiple parameters: -p"-DtableName=your-table-name -DcolumnName=your-column-name".'
                                       'Note: you should config in you job tableName with ${tableName}.')
    parser.add_option("-l", "--logdir", metavar="<log directory>",
                                  action="store", dest="logdir", type="string",
                                  help="the directory which log writes to",
                                  default=ADDAX_HOME + os.sep + 'log')

    parser.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")
    parser.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    return parser


def isUrl(path):
    if not path:
        return False

    assert (isinstance(path, str))
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False


def buildStartCommand(options, args):
    commandMap = {}
    tempJVMCommand = DEFAULT_JVM
    if options.jvmParameters:
        tempJVMCommand = tempJVMCommand + " " + options.jvmParameters

    if options.remoteDebug:
        tempJVMCommand = tempJVMCommand + " " + REMOTE_DEBUG_CONFIG

    if options.loglevel:
        tempJVMCommand = tempJVMCommand + " " + \
            ("-Dloglevel=%s" % (options.loglevel))

    # jobResource may be url , or local file(ralative, absolution)
    jobResource = args[0]
    if not isUrl(jobResource):
        jobResource = os.path.abspath(jobResource)
        if jobResource.lower().startswith("file://"):
            jobResource = jobResource[len("file://"):]
    # get job's filename if it's local file
    if not jobResource.startswith('http://') and not jobResource.startswith('https://'):
        jobFilename = os.path.splitext(os.path.split(jobResource)[-1])[0]
    else:
        jobFilename = jobResource[-20:].replace('/', '_').replace('.', '_')
    curr_time = time.strftime("%Y%m%d_%H%M%S")
    jobParams = ("-Daddax.log=%s -Dlog.file.name=addax_%s_%s_%s.log") % (
        options.logdir, jobFilename, curr_time, os.getpid())
    if options.params:
        jobParams = jobParams + " " + options.params

    commandMap["jvm"] = tempJVMCommand
    commandMap["params"] = jobParams
    commandMap["job"] = jobResource
    return Template(ENGINE_COMMAND).substitute(**commandMap)

def deprecated_warning():
    print("==================== DEPRECATED WARNING ========================")
    print("addax.py is deprecated, It's going to be removed in future release.")
    print("As a replacement, you can use addax.sh to run job")
    print("==================== DEPRECATED WARNING ========================")
    print()

if __name__ == "__main__":
    deprecated_warning()
    parser = getOptionParser()
    options, args = parser.parse_args(sys.argv[1:])
    if options.version:
        print(ADDAX_VERSION)
        sys.exit(0)

    if len(args) != 1:
        parser.print_help()
        sys.exit(RET_STATE['FAIL'])

    startCommand = buildStartCommand(options, args)
    if options.loglevel.lower() == "debug":
        print("start command: {}".format(startCommand))
    child_process = subprocess.Popen(startCommand, shell=True)
    register_signal()
    (stdout, stderr) = child_process.communicate()

    sys.exit(child_process.returncode)

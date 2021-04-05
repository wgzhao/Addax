#!/usr/bin/env python
from __future__ import print_function

import codecs
import json
import os
import platform
import re
import signal
import socket
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
    extract version from lib/datax-core-<version>.jar package
    """
    core_jar = glob(os.path.join(DATAX_HOME, "lib", "datax-core-*.jar"))
    if not core_jar:
        return ""
    else:
        return os.path.basename(core_jar[0])[11:-4]


DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATAX_VERSION = 'DataX ' + get_version()

if isWindows():
    codecs.register(
        lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    CLASS_PATH = "{1}{0}lib{0}*".format(os.sep, DATAX_HOME)
else:
    CLASS_PATH = "{1}{0}lib{0}*:.".format(os.sep, DATAX_HOME)
LOGBACK_FILE = "{1}{0}conf{0}logback.xml".format(os.sep, DATAX_HOME)
DEFAULT_JVM = "-Xms64m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath={}".format(
    DATAX_HOME)
DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
                        -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s " % \
                        (DATAX_HOME, LOGBACK_FILE)

if isWindows():
    ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.wgzhao.datax.core.Engine \
                -mode ${mode} -jobid ${jobid} -job ${job}" % \
                 (DEFAULT_PROPERTY_CONF,CLASS_PATH)
else:
    ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.wgzhao.datax.core.Engine \
                -mode ${mode} -jobid ${jobid} -job ${job}" % \
                 (DEFAULT_PROPERTY_CONF, "/etc/hbase/conf:" + CLASS_PATH)
    
REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"

RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    "OK": 0,
    "RUN": 1,
    "RETRY": 2
}


def getLocalIp():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


def suicide(signum, e):
    global child_process
    print("[Error] DataX receive unexpected signal {}, starts to suicide.".format(signum))

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    print("DataX Process was killed ! you did ?")
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

    prodEnvOptionGroup = OptionGroup(parser, "Product Env Options",
                                     "Normal user use these options to set jvm parameters, job runtime mode etc. "
                                     "Make sure these options can be used in Product Env.")
    prodEnvOptionGroup.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters", action="store",
                                  help="Set jvm parameters if necessary.")
    prodEnvOptionGroup.add_option("--jobid", metavar="<job unique id>", dest="jobid", action="store", default="-1",
                                  help="Set job unique id when running by Distribute/Local Mode.")
    prodEnvOptionGroup.add_option("-m", "--mode", metavar="<job runtime mode>",
                                  action="store", default="standalone",
                                  help="Set job runtime mode such as: standalone, local, distribute. "
                                       "Default mode is standalone.")
    prodEnvOptionGroup.add_option("-p", "--params", metavar="<parameter used in job config>",
                                  action="store", dest="params",
                                  help='Set job parameter, eg: the source tableName you want to set it by command, '
                                       'then you can use like this: -p"-DtableName=your-table-name", '
                                       'if you have mutiple parameters: -p"-DtableName=your-table-name -DcolumnName=your-column-name".'
                                       'Note: you should config in you job tableName with ${tableName}.')
    prodEnvOptionGroup.add_option("-r", "--reader", metavar="<parameter used in view job config[reader] template>",
                                  action="store", dest="reader", type="string",
                                  help='View job config[reader] template, eg: mysqlreader,streamreader')
    prodEnvOptionGroup.add_option("-w", "--writer", metavar="<parameter used in view job config[writer] template>",
                                  action="store", dest="writer", type="string",
                                  help='View job config[writer] template, eg: mysqlwriter,streamwriter')
    prodEnvOptionGroup.add_option("-l", "--logdir", metavar="<log directory>",
                                  action="store", dest="logdir", type="string",
                                  help="the directory which log writes to",
                                  default=DATAX_HOME + '/log')
    parser.add_option_group(prodEnvOptionGroup)

    devEnvOptionGroup = OptionGroup(parser, "Develop/Debug Options",
                                    "Developer use these options to trace more details of DataX.")
    devEnvOptionGroup.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")
    devEnvOptionGroup.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    parser.add_option_group(devEnvOptionGroup)
    return parser


def generateJobConfigTemplate(reader, writer):
    readerRef = "Please refer to the document:\n\thttps://datax.readthedocs.io/zh_CN/latest/reader/{}.html\n".format(
        reader)
    writerRef = "Please refer to the document:\n\thttps://datax.readthedocs.io/zh_CN/latest/writer/{}.html\n".format(
        writer)
    print(readerRef, writerRef)
    jobGuid = 'Please save the following configuration as a json file and  use\n     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json \nto run the job.\n'
    print(jobGuid)
    jobTemplate = {
        "job": {
            "setting": {
                "speed": {
                    "channel": ""
                }
            },
            "content": [
                {
                    "reader": {},
                    "writer": {}
                }
            ]
        }
    }
    readerTemplatePath = os.path.join(
        DATAX_HOME, "plugin", "reader", reader, "plugin_job_template.json")
    writerTemplatePath = os.path.join(
        DATAX_HOME, "plugin", "writer", writer, "plugin_job_template.json")
    readerPar = None
    writerPar = None
    try:
        readerPar = readPluginTemplate(readerTemplatePath)
    except Exception as e:
        print("Read reader[%s] template error: can\'t find file %s" % (
            reader, readerTemplatePath))
    try:
        writerPar = readPluginTemplate(writerTemplatePath)
    except Exception as e:
        print("Read writer[{}] template error: : can\'t find file {}: {}".format(
            writer, writerTemplatePath, e))
    jobTemplate['job']['content'][0]['reader'] = readerPar
    jobTemplate['job']['content'][0]['writer'] = writerPar
    print(json.dumps(jobTemplate, indent=4, sort_keys=True))


def readPluginTemplate(plugin):
    with open(plugin, 'r') as f:
        return json.load(f)


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
        print('local ip: ', getLocalIp())

    if options.loglevel:
        tempJVMCommand = tempJVMCommand + " " + \
            ("-Dloglevel=%s" % (options.loglevel))

    if options.mode:
        commandMap["mode"] = options.mode

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
    jobParams = ("-Ddatax.log=%s -Dlog.file.name=datax_%s_%s_%s.log") % (
        options.logdir, jobFilename, curr_time, os.getpid())
    if options.params:
        jobParams = jobParams + " " + options.params

    if options.jobid:
        commandMap["jobid"] = options.jobid

    commandMap["jvm"] = tempJVMCommand
    commandMap["params"] = jobParams
    commandMap["job"] = jobResource
    return Template(ENGINE_COMMAND).substitute(**commandMap)


if __name__ == "__main__":
    parser = getOptionParser()
    options, args = parser.parse_args(sys.argv[1:])
    if options.version:
        print(DATAX_VERSION)
        sys.exit(0)

    if options.reader is not None and options.writer is not None:
        generateJobConfigTemplate(options.reader, options.writer)
        sys.exit(RET_STATE['OK'])
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

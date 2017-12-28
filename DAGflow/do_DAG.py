#!/usr/bin/env python
"""
This script is used to submit DAG tasks from .json
The json can be created by another module DAG

Author: fan junpeng (jpfan@whu.edu.cn)
Version: V0.9
Last modified: 20171227

task status:
preparing  the job depends on other jobs, but not all of these jobs are running.
waiting    the job is waiting for submit to run due to max jobs
running    the job is submitted
success    the job was done and success
failed     the job was done but failed
"""

import os
import re
from collections import OrderedDict
import argparse
import sys
import logging
import time
import json
import signal
import subprocess


LOG = logging.getLogger(__name__)
TASKS = OrderedDict()
TASK_NAME = ""


def qhost():
    """

    :return:
    """
    r = {}
    contents = os.popen("qhost").read().strip().split("\n")

    for line in contents[2:]:
        line = line.strip()
        if len(line) == 0:
            continue

        content = line.split()
        _name = content[0]
        _status = content[8]

        if _status == "-":
            _status = "N"
        else:
            _status = "Y"

        r[_name] = _status

    return r


def qstat():
    """
    return a dict of qstat -u yourname
    :return:
    """

    r = {}
    user = os.popen('whoami').read().strip()
    contents = os.popen('qstat -u %s ' % user).read().strip().split('\n')

    # reading qstat content, the first 2 lines are passed
    for line in contents[2:]:
        content = line.split()

        _id = content[0]
        _status = content[4]

        # running jobs
        if "@" in content[7]:
            _node = content[7].split('@')[1]
        else:
            _node = ""

        r[_id] = {"status": _status,
                  "node": _node}

    return r


def check_done_task(task):

    if task["sge_option"]:
        task_out = task["sge_option"]["o"]
    else:
        task_out = task["local_option"]["o"]

    if os.path.isfile(task_out):

        with open(task_out) as out:
            content = out.read()

        if not re.search("task done", content):
            task["status"] = "failed"
        else:
            task["status"] = "success"
    else:
        task["status"] = "failed"

    return task


def update_task_status(tasks):
    running_task_stat = qstat()

    queue_status = qhost()
    died_queue = [i for i in queue_status if queue_status[i] == "N"]

    for task_id in tasks.keys():

        if "id" not in tasks[task_id]:
            tasks[task_id]["id"] = ""

        if "status" not in tasks[task_id]:
            tasks[task_id]["status"] = "preparing"

        _id = tasks[task_id]["id"]
        _status = tasks[task_id]["status"]

        # preparing tasks and waiting tasks
        if _status == "preparing":
            # check task depends, if a preparing task's depends submit, change stats to waiting
            dep_status = 1
            for dp in tasks[task_id]["depends"]:
                
                if tasks[dp]["status"] not in ["success"]:  # if the
                    dep_status = 0
                    break

            if dep_status:
                tasks[task_id]["status"] = "waiting"
            
            continue

        # pass success or failed task
        if _status in ["success", "failed", "waiting"]:
            continue

        # check recent done tasks on sge
        if _id and isinstance(_id, str) and _id not in running_task_stat:
            tasks[task_id] = check_done_task(tasks[task_id])
            continue

        # check tasks local
        if not isinstance(_id, str):
            task_status = _id.poll()

            # task is running
            if task_status is None:
                continue
            tasks[task_id] = check_done_task(tasks[task_id])
            continue

        # running task
        _status = running_task_stat[_id]["status"]
        _node = running_task_stat[_id]["node"]

        if _node in died_queue or _status == "Eqw":
            os.popen('qdel %s' % _id)
            LOG.debug("Delete job %r" % _id)

            tasks[task_id]["status"] = "failed"
    
    return tasks


def dict2str(params):
    """
    transform **params to real program param
     to
    :param params: params from test* eg: {"m": "a.fasta", "n": True, "i": False}
    :return: real param eg: "-query a.fasta -n "

    """
    params = dict(params)
    r = ""
    for param, value in params.items():

        if isinstance(value, bool):
            if value:
                r += " -%s " % param
                continue
        else:
            r += " -%s %s " % (param, value)

    return r


def qsub_tasks(tasks, concurrent_tasks):

    # limit the max concurrent_tasks
    if concurrent_tasks > 800:
        concurrent_tasks = 800

    running_tasks = OrderedDict()
    waiting_tasks = OrderedDict()

    for task_id in tasks:

        if tasks[task_id]["status"] == "running":
            running_tasks[task_id] = tasks[task_id]

        if tasks[task_id]["status"] == "waiting":
            waiting_tasks[task_id] = tasks[task_id]

    # job all submitted, pass
    if not waiting_tasks:
        return tasks

    task_num = concurrent_tasks - len(running_tasks)

    n = 0
    for task_id in waiting_tasks:

        if n < task_num:
            if tasks[task_id]["sge_option"]:
                qsub_cmd = "qsub -cwd %s %s" % (dict2str(tasks[task_id]["sge_option"]), tasks[task_id]["shell"])
                _id = os.popen(qsub_cmd).read().strip().split()[2]

                try:
                    int(_id)
                except:
                    LOG.error(_id)
                    raise Exception(_id)

                LOG.info("Submit task {task_id} with cmd '{qsub_cmd}'.".format(**locals()))
                tasks[task_id]["id"] = _id
                tasks[task_id]["status"] = "running"
                n += 1
                continue
            if tasks[task_id]["local_option"]:
                cmd = "sh %s" % tasks[task_id]["shell"]
                child = subprocess.Popen(
                    cmd,
                    stdout=open(tasks[task_id]["local_option"]["o"], "w"),
                    stderr=open(tasks[task_id]["local_option"]["e"], "w"),
                    shell=True
                )

                LOG.info("Running task {task_id} local.".format(**locals()))
                tasks[task_id]["id"] = child
                tasks[task_id]["status"] = "running"
                n += 1

    return tasks


def qdel_online_tasks(signum, frame):
    LOG.info("delete all running jobs, please wait")
    time.sleep(3)
    running_task_stat = qstat()

    for task_id in TASKS:

        if TASKS[task_id]["status"] != "running":
            continue

        _id = TASKS[task_id]["id"]

        if _id in running_task_stat:
            os.popen('qdel %s' % _id)
            LOG.info("Delete task %r id: %s" % (task_id, _id))
            TASKS[task_id]["status"] = "failed"

        # for local tasks
        if not isinstance(_id, str):
            try:
                _id.kill()
            except:
                pass

            LOG.info("Delete task %r id: %s" % (task_id, _id.pid))
            TASKS[task_id]["status"] = "failed"

    write_tasks(TASKS, TASK_NAME + ".json")

    sys.exit("sorry, the program exit")


def write_tasks(tasks, filename):
    failed_tasks = []

    for task_id in tasks:

        if tasks[task_id]["status"] != "success":
            failed_tasks.append(task_id)
            tasks[task_id]["status"] = "preparing"
            tasks[task_id]["id"] = ""

        if not isinstance(tasks[task_id]["id"], str):
            tasks[task_id]["id"] = "local"

    if failed_tasks:
        with open(filename, "w") as out:
            json.dump(tasks, out, indent=2)

        LOG.info("""\
The following tasks were failed:
%s
The tasks were save in %s, you can resub it.
                    """ % ("\n".join([i for i in failed_tasks]), filename))
        sys.exit("sorry, the program exit with some jobs failed")
    else:
        LOG.info("All jobs were done!")


def do_task(tasks, concurrent_tasks, refresh_time, log_name=""):

    start = time.time()

    logging.basicConfig(level=logging.DEBUG,
                        format="[%(levelname)s] %(asctime)s  %(message)s",
                        filename=log_name,
                        filemode='w',
                        )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s  %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    LOG.info("Start job.")

    global TASKS
    TASKS = tasks

    signal.signal(signal.SIGINT, qdel_online_tasks)
    signal.signal(signal.SIGTERM, qdel_online_tasks)
    # signal.signal(signal.SIGKILL, qdel_online_tasks)

    TASKS = update_task_status(TASKS)

    failed_json = TASK_NAME + ".json"

    loop = 0

    while 1:
        # qsub tasks
        TASKS = qsub_tasks(TASKS, concurrent_tasks)

        task_status = {
            "preparing": [],
            "waiting": [],
            "running": [],
            "success": [],
            "failed": []
        }

        for task_id in TASKS:
            status = TASKS[task_id]["status"]
            task_status[status].append(task_id)

        info = "job status: %s preparing %s waiting, %s running, %s success, %s failed." % (
            len(task_status["preparing"]),
            len(task_status["waiting"]), len(task_status["running"]),
            len(task_status["success"]), len(task_status["failed"]),
        )
        LOG.info(info)

        # all run
        if loop != 0 and len(task_status["running"]) == 0:
            break
        else:
            time.sleep(refresh_time)
            loop += 1
            TASKS = update_task_status(TASKS)

    # write failed
    write_tasks(TASKS, failed_json)
    totalTime = time.time() - start
    LOG.info('Total time:' + time.strftime("%H:%M:%S", time.gmtime(totalTime)))


def get_args():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""\
This script is used to submit DAG tasks from .json
The json can be created by another module DAG

Author: fan junpeng (jpfan@whu.edu.cn)
Version: V0.9
        """)

    parser.add_argument("json",  help="The json file contain DAG information")
    parser.add_argument("-m", "--max_task", type=int, default=200, help="concurrent_tasks")
    parser.add_argument("-r", "--refresh", type=int, default=30, help="refresh time of task status (seconds)")
    args = parser.parse_args()

    return args


def main():
    global TASK_NAME
    args = get_args()
    with open(args.json) as fh:
        tasks = json.load(fh, object_pairs_hook=OrderedDict)

    TASK_NAME = args.json.rstrip(".json")
    do_task(tasks, args.max_task, args.refresh, TASK_NAME+".log")


if __name__ == "__main__":
    main()


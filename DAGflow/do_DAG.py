#!/usr/bin/env python
"""
This script is used to submit DAG tasks from .json
The json can be created by another module DAG

Author: fan junpeng (jpfan@whu.edu.cn)
Version: V0.2
Last modified: 20171227

task status:
preparing  the job depends on other jobs, but not all of these jobs are running.
waiting    the job is waiting for submit to run due to max jobs
running    the job is submitted
success    the job was done and success
failed     the job was done but failed
"""

import os
from collections import OrderedDict
import argparse
import sys
import logging
import time
import json
import signal
from .DAG import Task, DAG


LOG = logging.getLogger(__name__)
TASKS = OrderedDict()
TASK_NAME = ""


def qhost():
    """
    "get the status of nodes"
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
    get the running jobs
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


def ps():
    r = []
    user = os.popen('whoami').read().strip()
    contents = os.popen('ps -u %s ' % user).read().strip().split('\n')

    for line in contents:
        r.append(line.split()[0])

    return r


def update_task_status(tasks):
    """

    :param tasks:
    :return:
    """
    sge_running_task = qstat()
    #local_running_task = ps()

    queue_status = qhost()
    died_queue = [i for i in queue_status if queue_status[i] == "N"]

    for id, task in tasks.items():

        # pass success or failed task
        if task.status in ["success", "failed", "waiting"]:
            continue

        # preparing tasks and waiting tasks
        if task.status == "preparing":
            # check task depends, if a preparing task's depends submit, change stats to waiting
            dep_status = 1

            for _id in task.depends:

                if tasks[_id].status != "success":
                    dep_status = 0
                    break

            if dep_status:
                task.status = "waiting"
            continue

        # check recent done tasks on sge
        if task.type == "sge" and task.run_id not in sge_running_task:
            task.check_done()
            continue
        elif task.type == "local":
            if task.run_id.poll():
                task.check_done()

            continue
        else:
            pass

        # check sge tasks running status
        _status = sge_running_task[task.run_id]["status"]

        if _status == "Eqw":
            task.kill()

        _node = sge_running_task[task.run_id]["node"]

        if _node in died_queue:
            task.kill()
            task.status = "preparing"
    
    return tasks


def submit_tasks(tasks, concurrent_tasks):

    # limit the max concurrent_tasks
    if concurrent_tasks > 800:
        concurrent_tasks = 800

    running_tasks = []
    waiting_tasks = []

    for id, task in tasks.items():

        if task.status == "running":
            running_tasks.append(task)

        if task.status == "waiting":
            waiting_tasks.append(task)

    # job all submitted, pass
    if not waiting_tasks:
        return tasks

    task_num = len(running_tasks)

    for task in waiting_tasks:
        task_num += 1

        if task_num > concurrent_tasks:
            break

        task.run()

    return tasks


def qdel_online_tasks(signum, frame):
    LOG.info("delete all running jobs, please wait")
    time.sleep(3)

    for id, task in TASKS.items():

        if task.status == "running":
            task.kill()

    write_tasks(TASKS, TASK_NAME + ".json")

    sys.exit("sorry, the program exit")


def write_tasks(tasks, filename):
    failed_tasks = []

    tasks_json = OrderedDict()

    for id, task in tasks.items():

        if task.status != "success":
            failed_tasks.append(task.id)

        tasks_json.update(task.to_json())

    with open(filename, "w") as out:
        json.dump(tasks_json, out, indent=2)

    if failed_tasks:
        LOG.info("""\
The following tasks were failed:
%s
The tasks were save in %s, you can resub it.
                    """ % ("\n".join([i for i in failed_tasks]), filename))
        sys.exit("sorry, the program exit with some jobs failed")
    else:
        LOG.info("All jobs were done!")


def do_dag(dag, concurrent_tasks, refresh_time, log_name=""):

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
    TASKS = dag.tasks

    signal.signal(signal.SIGINT, qdel_online_tasks)
    signal.signal(signal.SIGTERM, qdel_online_tasks)
    # signal.signal(signal.SIGKILL, qdel_online_tasks)

    for id, task in TASKS.items():
        task.init()

    failed_json = TASK_NAME + ".json"

    loop = 0

    while 1:
        # qsub tasks
        submit_tasks(TASKS, concurrent_tasks)

        task_status = {
            "preparing": [],
            "waiting": [],
            "running": [],
            "success": [],
            "failed": []
        }

        for id, task in TASKS.items():
            task_status[task.status].append(id)

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
            update_task_status(TASKS)

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

    TASK_NAME = os.path.splitext(os.path.basename(args.json))[0]
    print(TASK_NAME)
    dag = DAG.from_json(args.json)
    do_dag(dag, args.max_task, args.refresh, TASK_NAME+".log")


if __name__ == "__main__":
    main()


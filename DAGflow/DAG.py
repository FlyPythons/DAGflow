"""
Module to create DAG tasks
Author: fan junpeng(jpfan@whu.edu.cn)
Version: 0.9
"""

import os.path
from collections import OrderedDict
import json
import logging


LOG = logging.getLogger(__name__)


class DAG(object):

    def __init__(self, dag_id):
        self.id = dag_id
        self.task = []
        LOG.info("create DAG %r" % self.id)

    def add_task(self, *tasks):
        for task in tasks:
            self.task.append(task)

        return 1

    def add_dag(self, *dags):
        """
        add DAG object to DAG
        :param dags:
        :return:
        """
        depends = []

        # get all id of tasks is depended
        for task in self.task:
            depends += task.depends

        depends = set(depends)
        last_task = []

        # get tasks which is on the end of DAG
        for task in self.task:

            if task.id in depends:
                continue
            last_task.append(task)

        for dag in dags:
            assert isinstance(dag, DAG)

            for task in dag.task:

                if not task.depends:
                    task.set_upstream(*last_task)

            self.add_task(*dag.task)

        return 1

    def to_json(self):

        jsn = OrderedDict()
        for task in self.task:
            task.write_script()

            jsn[task.id] = task.to_json()

        fn = os.path.abspath("%s.json" % self.id)
        with open(fn, "w") as fh:
            json.dump(jsn, fh, indent=2)

        LOG.info("Write DAG %r tasks to %r" % (self.id, fn))

        return fn

    def print_task(self):

        for task in self.task:
            print(task.id, task.depends)

    def from_json(self):
        pass


class Task(object):

    def __init__(self, task_id, work_dir, script, type="sge", option={}):

        self.id = task_id
        self.work_dir = os.path.abspath(work_dir)
        self.script = script
        self.type = type
        self.option = option

        if "o" not in self.option:
            self.option["o"] = os.path.join(work_dir, "%s.STDOUT" % task_id)

        if "e" not in self.option:
            self.option["e"] = os.path.join(work_dir, "%s.STDERR" % task_id)

        self.script_path = os.path.join(work_dir, "%s.sh" % task_id)
        self.done = os.path.join(work_dir, "%s_done" % task_id)
        self.depends = []

    def write_script(self):
        script = """\
set -vex
hostname
date
cd {}
{}
touch {}
echo task done
date
""".format(self.work_dir, self.script, self.done)

        mkdir(self.work_dir)

        with open(self.script_path, "w") as fh:
            fh.write(script)

        return 1

    def to_json(self):
        sge_option = {}
        local_option = {}

        if self.type == "sge":
            sge_option = self.option
        if self.type == "local":
            local_option = self.option

        r = {
            "shell": self.script_path,
            "depends": self.depends,
            "sge_option": sge_option,
            "local_option": local_option
        }

        return r

    def set_downstream(self, *tasks):

        for task in tasks:
            task.depends.append(self.id)

        return 1

    def set_upstream(self, *tasks):

        for task in tasks:
            self.depends.append(task.id)

        return 1


def mkdir(d):
    """
    from FALCON_KIT
    :param d:
    :return:
    """
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        LOG.debug('mkdir {!r}'.format(d))
        os.makedirs(d)
    else:
        LOG.debug('mkdir {!r}, {!r} exist'.format(d, d))

    return d

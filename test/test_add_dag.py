#!/usr/bin/env python
"""
test on DAG.add_dag
"""


from DAGflow import *

workflow = DAG("test")

task1 = Task(
    id="task1",
    work_dir="",
    type="sge",
    script="echo running workflow task 1\nsleep 5"
)

workflow.add_task(task1)

task2 = Task(
    id="task2",
    work_dir="task2",
    type="local",
    script="echo running workflow task 2\n sleep 5"
)
workflow.add_task(task2)
task2.set_upstream(task1)

do_dag(workflow, 200, 10, "test")
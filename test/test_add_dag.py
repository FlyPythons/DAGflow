#!/usr/bin/env python
"""
test on DAG.add_dag
"""


from DAGflow.DAG import *

workflow = DAG("test")

task1 = Task(
    task_id="task1",
    work_dir="",
    type="sge",
    script="echo running workflow task 1"
)

workflow.add_task(task1)

task2 = Task(
    task_id="task2",
    work_dir="task2",
    type="local",
    script="echo running workflow task 3"
)
workflow.add_task(task2)
task2.set_upstream(task1)

wf2 = DAG("test2")

task3 = Task(
    task_id="task3",
    work_dir=".",
    type="local",
    script="echo running workflow task 3"
)

wf2.add_task(task3)

workflow.add_dag(wf2)

workflow.print_task()

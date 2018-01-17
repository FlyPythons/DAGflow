#!/usr/bin/env python
import os
import sys

from DAGflow import DAG, Task

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
    script="echo running workflow task 1"
)
workflow.add_task(task2)
task2.set_upstream(task1)

# all of you tasks were added to you workflow, you can run it
# write you workflow to a json file
js = workflow.to_json()
# submit you workflow tasks with do_DAG
m = os.system("python -m DAGflow.do_DAG %s" % js)

if m == 256:
    print("Something wrong with this module")
    sys.exit("program exit")

print("Test passed, Congratulations!")

# DAGflow
a python Module to create DAG Task and Manage Task on SGE
## Installation
### 1. Requirements
Python (2.5 or later)  
Sun Grid Engine(SGE)

DAGflow has been tested on CentOS 7.2.1511 and Ubuntu 12.04.4 LTS
### 2. Install
Download the code and unzip it into desired installation directory 
```commandline
git clone https://github.com/FlyPythons/DAGflow.git
```

## Tutorial
The following tutorial shows how to create a DAGflow and run it
### 1. Draw you work flow
Now i have a set of fasta files and i want to blast them to a db named 'db.fasta'.  
To complete this work, a workflow as following is needed
![image](https://github.com/FlyPythons/DAGflow/raw/master/test/workflow.jpg)
### 2. Write your workflow script
At first, you should write your workflow script 
```python
import os
from dagflow import DAG, Task, ParallelTask, do_dag


inputs = ['1.fasta', "2.fasta", "3.fasta", "4.fasta"]
db = "db.fasta"
db = os.path.abspath(db)

# create a DAG object
my_dag = DAG("blast")
# create the first task 'make_db'
make_db = Task(
    id="make_db",  # your task id, should be unique
    work_dir=".",  # you task work directory
    type="local",  # the way your task run. if "sge", task will submit with qsub
    option={},  # the option of "sge" or "local"
    script="makeblastdb -in %s -dbtype nucl" % db  # the command of the task
)

# when you create a task, then add it to DAG object
my_dag.add_task(make_db)

# then add blast tasks
blast_tasks = ParallelTask(id="blast",
                            work_dir="{id}",
                            type="sge",
                            option="-pe smp 4 -q all.q",
                            script="blastn -in {query} -db %s -outfmt 6 -out {query}.m6",
                            query=inputs)

my_dag.add_task(*blast_tasks)
make_db.set_downstream(*blast_tasks)
# add blast_join task to join blast results
blast_join = Task(
    id="blast_join",
    work_dir=".",
    type="local",  # option is default
    script="cat */*.m6 > blast.all.m6"
)
# you should always remember to add you task to DAG object when created
my_dag.add_task(blast_join)
# this task need a list of tasks in blast_task all done
blast_join.set_upstream(*blast_tasks)

# all of you tasks were added to you workflow, you can run it
do_dag(my_dag)

```
Now, your workflow script is completed, you can name it as 'workflow.py'
### 3. Run you workflow 
You can run you workflow script as a python script using the following commands.
```commandline
python workflow.py
```
### Re-run your workflow if it was break in the middle
For some reason, you workflow was broken with some tasks undone.  
You can use the same command `python workflow.py`  to re-run the undone jobs.
### set dependence between workflow and task
Sometimes you may want to add a workflow to another workflow, this can be down as following:  
```python
from DAGflow import *


# two workflow wf1 and wf2
wf1 = DAG("workflow1")
wf2 = DAG("workflow2")
task1 = Task(
    id="task",
    work_dir=".",
    script="hello, i am a task"
)

# set wf2 depends on wf1
wf1.add_dag(wf2)

# set task1 depends on wf2
task1.set_upstream(wf2.tasks.values())
```

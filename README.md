# DAGflow
## Installation
### Requirements
Python (2.5 or later)  
SGE  

DAGflow has been tested on CentOS and ubtune
### Get the code
Download the code and unzip it into desired installation directory 
```commandline
mkdir /usr/local/pythonlib
cd /usr/local/pythonlib
wget 
unzip DAGflow.zip
```
### Test installation
Add the installation directory to your PYTHONPATH.
```commandline
export PYTHONPATH=/usr/local/pythonlib/:$PYTHONPATH
```
Test
```commandline
cd /usr/local/pythonlib/DAGflow/test
python test.py

```
if you see "Test passed, Congratulations!", the test passed.
## Tutorial
The following tutorial shows how to create a DAGflow and run it
### Draw you work flow
Now i have a set of fasta files and i want to blast them to a db named 'db.fasta'.  
To complete this work, a workflow as following is needed
![image](https://raw.githubusercontent.com/wiki/FlyPythons/DAGflow/test/workflow.jpg)
### Write your workflow script
At first, you should write your workflow script 
```python
import os
from DAGflow.DAG import DAG, Task


inputs = ['1.fasta', "2.fasta", "3.fasta", "4.fasta"]
db = "db.fasta"
db = os.path.abspath(db)

# create a DAG object
my_dag = DAG("blast")
# create the first task 'make_db'
make_db = Task(
    task_id="make_db",  # your task id, should be unique
    work_dir="",  # you task work directory
    type="local",  # the way your task run. if "sge", task will submit with qsub
    option={},  # the option of "sge" or "local"
    script="makeblastdb -in %s -dbtype nucl" % db  # the command of the task
)

# when you create a task, then add it to DAG object
my_dag.add_task(make_db)

# then add blast tasks
blast_tasks = []  # a list to obtain blast tasks

n = 1
for fn in inputs:
    task_id = "blast_%s" % n
    task = Task(
        task_id= task_id,
        work_dir=task_id,
        type="sge", 
        option={
            "pe": "smp 4",
            "q": "all.q"
        },
        script="blastn -in %s -db %s -outfmt 6 -out %s" % (fn, db, "%s.m6" % task_id)
    )
    
    my_dag.add_task(task)
    # set the upstream of the task, this means this task required 'make_db' done
    task.set_upstream(make_db)
    blast_tasks.append(task)
    n += 1

# add blast_join task to join blast results
blast_join = Task(
    task_id="blast_join",
    work_dir="",
    type="local",  # option is default
    script="cat */*.m6 > blast.all.m6"
)
# you should always remember to add you task to DAG object when created
my_dag.add_task(blast_join)
# this task need a list of tasks in blast_task all done
blast_join.set_upstream(*blast_tasks)

# all of you tasks were added to you workflow, you can run it
# write you workflow to a json file
js = my_dag.to_json()
# submit you workflow tasks with do_DAG
os.system("python -m DAGflow.do_DAG %s" % js)
```
Now, your workflow script is completed, you can name it as 'workflow.py'
### Run you workflow 
You can run you workflow script as a python script using the following commands.
```commandline
python workflow.py
```
### Re-run your workflow if it was break in the middle
For some reason, you workflow was broken with some tasks undone.  
You can use the following commands to re-run the undone jobs.
```commandline
python -m DAGflow.do_DAG blast.json 
# note that the blast.json is in you work directory, 'blast' is your DAG id.
```
### Add workflow to workflow
Sometimes you may want to add a workflow to another workflow, this can be down as following:  
```python
from DAGflow.DAG import *


# two workflow wf1 and wf2
wf1 = DAG("workflow1")
wf2 = DAG("workflow2")

# you can add task to these 2 workflow

# you can add workflow2 to the end of workflow1 as following
wf1.add_dag(wf2)
```

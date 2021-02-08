# toy-ruffus-pipeline

Toy pipeline for testing simple concepts in Ruffus framework


### Dependencies

 - `pip install ruffus`
 
If testing drmaa_wrapper:

 - `pip install drmaa` (OS's slurm_drmaa library will be necessary)
 
 

### Example runs:

`python main.py --input_path /tmp/toy --count 10 -j 100 --verbose 2 --target_tasks complete_run`

`python main.py --input_path /tmp/toy --count 1000 -j 2000 --use_threads --verbose 2 --target_tasks complete_run`

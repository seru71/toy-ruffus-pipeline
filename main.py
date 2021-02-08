# This is a sample Python script.

import os
import pathlib
import shutil
import ruffus.cmdline as cmdline
from ruffus import originate, transform, suffix, mkdir, posttask, follows
from ruffus.drmaa_wrapper import run_job, error_drmaa_job

#
# Uncomment if you need to test drmaa (needs installation of OS package: slurm-drmaa)
#
# try:
#     import drmaa
#     session = drmaa.Session()
#     session.initialize()
# except Exception:
#     pass
# except OSError:
#     pass


def run_slurm_job(cmd, **args):
    try:
        stdout_res, stderr_res = run_job(cmd,
                                         logger=logger,
                                         drmaa_session=session,
                                         run_locally=False,
                                         **args)

    # relay all the stdout, stderr, drmaa output to diagnose failures
    except error_drmaa_job as err:
        raise Exception("\n".join(map(str,
                                      ["Failed to run:",
                                       cmd,
                                       err,
                                       stdout_res,
                                       stderr_res])))


parser = cmdline.get_argparse(description='WHAT DOES THIS PIPELINE DO?',
                              ignored_args=['checksum_file_name'])

#   <<<---- add your own command line options like --input_file here
parser.add_argument("--input_path", dest="input_path", type=pathlib.Path)
parser.add_argument("--count", dest="input_count", type=int)
options = parser.parse_args()

# create list of input files based on the options.input_count
input_files = [os.path.join(options.input_path, 'file_' + str(i) + "_in") for i in range(1, options.input_count + 1)]


@mkdir(os.path.dirname(input_files[0]))
@originate(input_files)
def create_input_file(fpath):
    """ Create file with its numeric suffix as content
    :param fpath: path to file to create
    :return: void
    """
    with open(fpath, 'w') as f:
        f.write(os.path.basename(fpath).split("_")[1])
        f.write("\n")


@transform(create_input_file, suffix("_in"), "_out1")
def process_input_in_python(inputf, outputf):
    """
    Square the value read from inputf and save in outputf
    :param inputf: file to read number from
    :param outputf: file to save squere to
    :return: void
    """
    value=-1
    with open(inputf, 'r') as f:
        value = int(f.readline().strip())
    #print(outputf)
    time.sleep(5)
    with open(outputf, 'w') as f:
        f.write(str(value**2))


@transform(create_input_file, suffix("_in"), "_out2")
def process_input_using_awk(inputf, outputf):
    """
    Square the value read from inputf and save in outputf
    :param inputf: file to read number from
    :param outputf: file to save squere to
    :return: void
    """
    cmd = "sleep 5; awk '{print $1*$1}' " + ("{} > {}").format(inputf, outputf)
    print(outputf)
    run_job(cmd, run_locally=True)
    #run_slurm_job(cmd) # will need drmaa and running on a SLURM submission node


def rm_path():
    """
    Remove input directory options.input_path
    :return: void
    """
    shutil.rmtree(options.input_path)


@follows(process_input_in_python, process_input_using_awk)
#@posttask(lambda: shutil.rmtree(options.input_path))
@posttask(rm_path)
def complete_run():
    """ Aggregates the two processing tasks """
    pass


#  standard python logger which can be synchronised across concurrent Ruffus tasks
logger, logger_mutex = cmdline.setup_logging(__name__, options.log_file, options.verbose)

import time
start = time.time()
cmdline.run(options, checksum_level=0)
end = time.time()
print(end - start)

#
# Uncomment if you need to test drmaa (needs installation of OS package: slurm-drmaa)
#
# try:
#     session.exit()
# except Exception:
#     pass

#
# Example runs:
#
# python main.py --input_path /tmp/toy --count 10   -j 100                --verbose 2 --target_tasks complete_run
# python main.py --input_path /tmp/toy --count 100  -j 200  --use_threads --verbose 2 --target_tasks complete_run
# python main.py --input_path /tmp/toy --count 100  -j 2000 --use_threads --verbose 2 --target_tasks complete_run
# python main.py --input_path /tmp/toy --count 1000 -j 2000 --use_threads --verbose 2 --target_tasks complete_run
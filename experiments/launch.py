#!/usr/bin/env python

import click
import subprocess as sp
from random import shuffle
from json import load
from os import linesep, makedirs, path as op
from time import time, sleep


def wait(job_id):
    print(f"Waiting for job {job_id} termination")

    while True:
        p = sp.Popen(
            ["squeue", "--job", job_id], stdout=sp.PIPE, stderr=sp.PIPE
        )
        out, err = p.communicate()

        out = out.decode("utf-8")
        err = err.decode("utf-8")

        if len(out.strip(linesep).split("\n")) <= 1:
            print(f"Slurm job {job_id} completed")
            break

        else:
            sleep(10)


def launch(sbatch_file, nodelist):

    job_id = None

    if nodelist is None:
        cmd = ["sbatch", sbatch_file]
    else:
        cmd = ["sbatch", f"--nodelist={nodelist}", sbatch_file]

    print("Executing command", " ".join(cmd))

    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = p.communicate()

    out = out.decode("utf-8")
    err = err.decode("utf-8")

    if out != "":
        job_id = out.strip(linesep).split(" ")[-1]

    print("Output:", out, linesep)
    print("Error:", err, linesep)
    print("Command completed.\n")

    return job_id


def gen_sbatch(exp, results_dir):

    # convert mem to megabytes
    exp["mem"] *= 1000

    # need memory limit in bytes to pass to the keep
    memory_bytes = exp["mem"] * 1000 ** 2
    sbatch_file = op.join(results_dir, f"sbatch_{exp['name']}.sh")
    log_file = op.join(results_dir, "logs.csv")

    with open(log_file, "w") as f:
        f.write(
            "Seeks, peak memory (B), read time (s),  write time (s), elapsed time (s)\n"
        )

    # create sbatch file for launching script
    template = (
        f"#!/bin/bash\n"
        f"#SBATCH --account={exp['account']}\n"
        f"#SBATCH --job-name={exp['name']}\n"
        f"#SBATCH --nodes=1\n"
        f"#SBATCH --mem={exp['mem']}\n"
        f"#SBATCH --output={results_dir}/slurm-%x-%j.out\n"
        f"\n\n"
        f"rm -rf {exp['cwd']}\n"
        f"mkdir {exp['cwd']}\n"
        f"cd {exp['cwd']}\n"
        f"\n\n"
        f'echo "Clearing cache" && sync && echo 3 | sudo tee /proc/sys/vm/drop_caches\n'
        f"source {exp['venv']}\n"
        f"export KEEP_LOG={log_file}\n"
        f"\n\n"
        f"repartition --max-mem {memory_bytes} --create  \"{exp['a']}\" \"{exp['i']}\" \"{exp['o']}\" {exp['alg']}\n"
        f"\n"
        f"start=`date +%s.%N`\n"
        f"repartition --max-mem {memory_bytes} --repartition \"{exp['a']}\" \"{exp['i']}\" \"{exp['o']}\" {exp['alg']}\n"
        f"end=`date +%s.%N`\n"
        f"\n"
        f"repartition --max-mem {memory_bytes} --delete \"{exp['a']}\" \"{exp['i']}\" \"{exp['o']}\" {exp['alg']}\n"
        f"\n\n"
        f'runtime=$( echo "$end - $start" | bc -l)\n'
        f"echo \"Runtime: $runtime\" > {op.join(results_dir, 'runtime.txt')}\n"
        f'echo "Removing directories"\n'
        f"rm -rf {exp['cwd']}"
    )

    with open(sbatch_file, "w+") as f:
        f.write(template)

    return sbatch_file


@click.command()
@click.argument("conditions", type=click.File("r"))
@click.argument("repetitions", type=int)
@click.argument("results_dir", type=click.Path())
@click.option("--nodelist", type=str, default=None)
def main(conditions, repetitions, results_dir, nodelist):

    # randomize experiments
    rand_exp = load(conditions)
    shuffle(rand_exp)

    results_dir = op.abspath(
        op.join(results_dir, f"execution-{str(int(time()))}")
    )

    for i in range(repetitions):
        for exp in rand_exp:

            it_dir = op.join(results_dir, f"run-{i}", exp["name"])
            print("Creating output directory:", it_dir)
            makedirs(it_dir)

            # setup sbatch script
            sb_file = gen_sbatch(exp, it_dir)

            # launch experiments
            job_id = launch(sb_file, nodelist)

            # wait for experiment to complete
            wait(job_id)


if __name__ == "__main__":
    main()

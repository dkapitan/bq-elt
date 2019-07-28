import asyncio
import yaml
from collections import namedtuple
from datetime import datetime


Job = namedtuple('Job', ['name', 'input', 'output', 'job_id'])
with open('jobs.yaml', 'r') as f:
    _jobs = yaml.load(f, Loader=yaml.FullLoader)
jobs = [
    Job(key, value['input'], value['output'], None)
    for job in _jobs['jobs']
    for key, value in job.items()
]
started = []
done = []


async def await_and_start_job(job):
    """
    Args:
        job: NamedTuple(name, input, output, job_id)
    """
    global started

    def start(job):
        job_id = '_'.join([job.name, datetime.now().isoformat()])
        print(f'Started {job.name} with job_id {job_id}')
        started.append(Job(*job[0:3], job_id))

    def await_input(job):
        # TO_DO: need to await from started here
        # add callback
        # when done push to done
        pass

    # job can start immmediately if no inputs
    if not job.input:
        start(job)

    elif job.input:
        await_input(job)
        start(job)


async def main():
    # start tasks for jobs with no input
    tasks = {
        '-'.join(['task', job.name]):
        asyncio.create_task(await_and_start_job(job))
        for job in jobs
        if not job.input
    }
    for _, task in tasks.items():
        await task

    # while not all jobs are started, keep starting tasks
    while len(started) < len(jobs):
        _queque = (
            set([job.name for job in jobs]) -
            set([job.name for job in started])
        )
        print('tasks: ', tasks)
        print('queue', _queque)
        queque = [job for job in jobs if job.name in _queque]
        for job in queque:
            _outputs = set(
                [output for job in started for output in job.output]
            )
            print(_outputs)
            if set(job.input).issubset(_outputs):
                tasks['-'.join(['task', job.name])] = asyncio.create_task(
                    await_and_start_job(job)
                )
                await tasks['-'.join(['task', job.name])]
            else:
                asyncio.sleep(1)
    print(started)


if __name__ == '__main__':
    asyncio.run(main())

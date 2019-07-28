import asyncio
import time
from google.cloud.bigquery import Client


async def await_jobs(job_ids, polling_delay=1):
    """
    Await set of job_ids
    """
    def callback(future):
        awaiting_jobs.discard(future.id)

    while job_ids:
        print('waiting for jobs to finish ... sleeping for 1s')
        asyncio.sleep(1)


bq = Client('mediquest-sandbox')
query_1 = """
    SELECT
      language.name,
      average(language.bytes)
    FROM `bigquery-public-data.github_repos.languages` 
    , UNNEST(language) AS language
    GROUP BY language.name
"""
query_2 = 'SELECT 2'
queries = [query_1, query_2]


def main2():
    jobs = set()
    for query in queries:
        job = bq.query(query)
        jobs.add(job.job_id)
        job.add_done_callback(await_jobs.callback)



    print('all jobs done, do your stuff')




async def say_after(delay, what):
    await asyncio.sleep(delay)
    print(what)


async def main():
    print(f"started at {time.strftime('%X')}")

    await say_after(1, 'hello')
    await say_after(2, 'world')

    print(f"finished at {time.strftime('%X')}")


if __name__ == '__main__':
    # asyncio.run(main())
    main2()


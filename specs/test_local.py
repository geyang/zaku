from asyncio import sleep, run

from tqdm import trange

from jobq.queue import JobQueue

job_queue = JobQueue()

for i in range(100):
    job_queue.append({"param_1": i * 100, "param_2": f"key-{i}"})


async def main(queue: JobQueue):
    await sleep(0.0)

    # here is the job handling logic: might want to switch to a context manager.
    key, mark_done, put_back = queue.take()
    try:
        print(f"I took job-{key}.")
        print(""" Put your work over here. """)
        for step in trange(100, desc="iterate through job steps"):
            # update scene with params:
            await sleep(0.02)
            # uncomment the following line to grab the rendering result.
            # result = await proxy.grab_render(downsample=1, key="ego")

        print("Job is completed.")
        # now the job has been finished, we mark it as done by removing it from the queue.
        mark_done()
    except:
        print("Oops, something went wrong. Putting the job back to the queue.")
        put_back()


run(main(job_queue))

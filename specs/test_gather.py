import pytest

from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue")
task_queue.init_queue()


def worker_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name)

    job = None

    for i in range(10):
        with queue.pop() as job:
            if job is None:
                sleep(0.5)
                continue

            # these are boilerplates that we prob want to remove from the user's space.
            gather_queue_name = job.pop("_gather_id")  # "gather id must be in"
            gather_queue = TaskQ(name=gather_queue_name)
            # print("gather queue name", gather_queue_name)
            gather_token = job.pop("_gather_token")  # _gather_token must be in.

            # we simulate a long-running job. Make sure you clear the queue first though.
            sleep(0.1)
            # print(job['seed'])
        # we return the result to the response topic.
        # can be called return gather or something
        # print("finished job", job)
        gather_queue.add({"_gather_token": gather_token})
        # gather_queue.add({})

    print("process finished")


@pytest.mark.dependency(name="test_gather")
def test_gather():
    """adding"""
    from multiprocessing import Process

    queue_name = "ZAKU_TEST:debug-gather-queue"
    job_queue = TaskQ(name=queue_name)
    # this is important, otherwise the worker will get suck with
    # an old message.
    job_queue.clear_queue()

    # start five worker processes
    procs = []
    for i in range(10):
        p = Process(target=worker_process, args=(queue_name,))
        p.start()
        procs.append(p)

    jobs = [dict(seed=i) for i in range(20)]
    is_done, tokens = job_queue.gather(jobs)

    print("waiting...")
    assert is_done(blocking=True), "done should mark to be True"
    print("is done!")

    jobs = [dict(seed=i) for i in range(300)]
    is_done, tokens = job_queue.gather(jobs)

    print("waiting...")
    assert not is_done(blocking=False), "done should not be marked to be True"

    for p in procs:
        p.join()

@pytest.mark.dependency(name="test_gather_imperative")
def test_gather_imperative():
    """adding"""
    import time
    from multiprocessing import Process

    print()

    print("setting up queue")

    queue_name = "ZAKU_TEST:debug-gather-queue"
    job_queue = TaskQ(name=queue_name)
    # this is important, otherwise the worker will get suck with
    # # an old message.
    job_queue.clear_queue()

    print("this is here")

    # start five worker processes
    procs = []
    for i in range(4):
        p = Process(target=worker_process, args=(queue_name,))
        p.start()
        procs.append(p)

    print("===================")
    jobs = [dict(seed=i) for i in range(30)]
    tokens = None
    for j in jobs:
        is_done, tokens = job_queue.gather_one(j, tokens)
    print("done adding")
    print("waiting...")

    while not is_done(blocking=False):
        print("counts", len(tokens), "total left", len(job_queue))
        time.sleep(0.1)

    print("job_queue length:", len(job_queue))
    assert is_done(blocking=True), "done should mark to be True"
    print("is done!")

    for p in procs:
        p.join()

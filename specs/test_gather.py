import pytest

from zaku import TaskQ

task_queue = TaskQ(name="ZAKU_TEST:debug-queue", uri="http://localhost:9000")
task_queue.init_queue()


def worker_process(queue_name):
    from time import sleep

    queue = TaskQ(name=queue_name, uri="http://localhost:9000")

    job = None

    for i in range(10):
        with queue.pop() as job:
            if job is None:
                sleep(0.1)
                continue

            # these are boilerplates that we prob want to remove from the user's space.
            gather_queue_name = job.pop("_gather_id")  # "gather id must be in"
            gather_queue = TaskQ(name=gather_queue_name)
            # print("gather queue name", gather_queue_name)
            gather_token = job.pop("_gather_token")  # _gather_token must be in.

            # we simulate a long-running job. Make sure you clear the queue first though.
            sleep(0.01)

        # we return the result to the response topic.
        # can be called return gather or something
        # print("finished job", job)
        gather_queue.add({"_gather_token": gather_token})
        # gather_queue.add({})

    print("process finished")


@pytest.mark.dependency(name="test_rpc")
def test_gather():
    """adding"""
    from multiprocessing import Process

    queue_name = "ZAKU_TEST:debug-gather-queue"
    job_queue = TaskQ(name=queue_name, uri="http://localhost:9000")
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


@pytest.mark.dependency(name="test_rpc")
def test_gather_imperative():
    """adding"""
    from multiprocessing import Process

    queue_name = "ZAKU_TEST:debug-gather-queue"
    job_queue = TaskQ(name=queue_name, uri="http://localhost:9000")
    # this is important, otherwise the worker will get suck with
    # an old message.
    job_queue.clear_queue()

    # start five worker processes
    procs = []
    for i in range(40):
        p = Process(target=worker_process, args=(queue_name,))
        p.start()
        procs.append(p)

    jobs = [dict(seed=i) for i in range(100)]
    tokens = None
    for j in jobs:
        is_done, tokens = job_queue.gather_one(j, tokens)

    print("waiting...")
    assert is_done(blocking=True), "done should mark to be True"
    print("is done!")

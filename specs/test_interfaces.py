from jobq.interfaces import Job


def test_job_interface():
    import msgpack

    job = Job(created_ts=0, status=None, grab_ts=0, value="hello")
    msg = job.serialize()
    job_new = Job.deserialize(msg)
    print("job reconstructed", job_new)
    print(job.serialize())
    print(msgpack.unpackb(job.serialize(), raw=False))

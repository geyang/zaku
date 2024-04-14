import os
from contextlib import nullcontext

from cmx import doc

MAKE_DOCS = os.getenv("MAKE_DOCS", None)

doc @ """
# Zaku Example

This example shows you how upload and download data

"""
# Need to use this hack to make the code works for python < 3.9
with doc, doc.skip if MAKE_DOCS else nullcontext():
    from zaku import JobQ

    queue = JobQ()

doc.print(queue)

doc @ """
Now we can add a few jobs to the queue
"""
with doc, doc.skip if MAKE_DOCS else nullcontext():
    queue.add({"seed": 100, "data": [1, 2, 3]})

doc @ """
If you want more control over the job id, you can specify it explicitly using the key argument:
"""
with doc, doc.skip if MAKE_DOCS else nullcontext():
    count = 0
    queue.add({"seed": 200, "data": [1, 2, 3]}, key=count)

with doc, doc.skip if MAKE_DOCS else nullcontext():
    job_id, job_config = queue.take()

doc.print(job_id, job_config)

doc @ """
Now if we take again, it gives us a different one
"""
with doc, doc.skip if MAKE_DOCS else nullcontext():
    job_id, job_config = queue.take()

doc.print(job_id, job_config)

doc @ """
Since we only added two jobs, the queue should be empty now. Let's check!
"""
with doc, doc.skip if MAKE_DOCS else nullcontext():
    result = queue.take()

    doc.print(f"result is: [{result}]")

<h1 class="full-width" style="font-size: 49px">Welcome to <code style="font-family: sans-serif; background-clip: text; color: transparent; background-image: linear-gradient(to right, rgb(96 218 190), rgb(250 131 11), rgb(255 90 66));">JobQ</code><span style="font-size: 0.3em; margin-left: -0.5em; margin-right:-0.4em;">｣</span></h1>

<link rel="stylesheet" href="_static/title_resize.css">

JobQ is a light-weight job queue backed by redis for machine learning workloads. 

```shell
pip install jobq[all]=={VERSION}
```

Here is an example of how to add and retrieve jobs from JobQ. 
For a more comprehensive list of examples, please refer to the [examples](examples/01_simple_queue) page.

```python
from jobq import JobQ

app = JobQ(host="localhost", port=8000**)

while True:
    job = app.take()
    if job is None:
        continue
    print(job)
    app.add_job(job)
```

JobQ is built by researchers at MIT in fields including robotics, computer vision, and computer graphics.

- light-weight and performant
- scalable and versatile.
- Open source, licensed under MIT

To get a quick overview of what you can do with  <code style="font-size: 1.3em; background-clip: text; color: transparent; background-image: linear-gradient(to right, rgb(96 218 190), rgb(250 131 11), rgb(255 90 66));">jobq</code>, check out the following:

- take a look at the basic tutorial or the tutorial for robotics:
  - [JobQ Basics](tutorials/basics)
  - [Tutorial for Roboticists](tutorials/robotics)
- or try to take a look at the example gallery [here](examples/01_simple_queue)

For a comprehensive list of visualization components, please refer to
the [API documentation on Components | jobq](https://docs.jobq.ai/en/latest/api/jobq.html).

For a comprehensive list of data types, please refer to the [API documentation on Data Types](https://docs.jobq.ai/en/latest/api/types.html).


<!-- prettier-ignore-start -->

```{eval-rst}
.. toctree::
   :hidden:
   :maxdepth: 1
   :titlesonly:

   Quick Start <quick_start>
   Report Issues <https://github.com/geyang/jobq/issues?q=is:issue+is:closed>
   CHANGE LOG <CHANGE_LOG.md>
   
.. toctree::
   :maxdepth: 3
   :caption: Tutorials
   :hidden:
   
   tutorials/basics.md
   tutorials/setting_up_server.md
   
.. toctree::
   :maxdepth: 3
   :caption: Examples
   :hidden:
   
   Simple Example <examples/01_simple_queue.md>
   
.. toctree::
   :maxdepth: 3
   :caption: Python API
   :hidden:
   
   jobq JobQ Client <api/jobq.md>
   jobq.base <api/base.md>
   jobq.interfaces — Type Interafce <api/interfaces.md>
   jobq.server — JobServer <api/server.md>
    
```

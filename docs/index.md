<h1 class="full-width" style="font-size: 49px"><code style="font-family: sans-serif; background-clip: text; color: transparent; background-image: linear-gradient(to right, rgb(255 139 128), rgb(208 6 27), rgb(97 12 0));">Zaku</code> Task Queue<span style="font-size: 0.3em; margin-left: 0.5em; margin-right:-0.4em;">｣</span></h1>

<link rel="stylesheet" href="_static/title_resize.css">

Zaku is a light-weight job queue backed by redis for machine learning workloads. 

**To Install:** note the single quote `'` around the bracket for `zsh`.
```shell
pip install 'zaku[all]=={VERSION}'
```

Here is an example of how to add and retrieve jobs from Zaku. 
For a more comprehensive list of examples, please refer to the [examples](examples/01_simple_queue) page.

```python
from zaku import TaskQ

app = TaskQ(uri="http://localhost:9000")

while True:
    job = app.take()
    if job is None:
        continue
    print(job)
    app.add_job(job)
```

Zaku is built by researchers at MIT in fields including robotics, computer vision, and computer graphics.

- light-weight and performant
- scalable and versatile.
- Open source, licensed under MIT

To get a quick overview of what you can do with  <code style="font-size: 1.3em; background-clip: text; color: transparent; background-image: linear-gradient(to right, rgb(255 139 128), rgb(208 6 27), rgb(97 12 0));">zaku</code>, check out the following:

- take a look at the basic tutorial or the tutorial for robotics:
  - [Zaku Basics](tutorials/basics)
  - [Tutorial for Roboticists](tutorials/robotics)
- or try to take a look at the example gallery [here](examples/01_simple_queue)

For a comprehensive list of visualization components, please refer to
the [API documentation on Components | zaku](https://docs.zaku.ai/en/latest/api/zaku.html).

For a comprehensive list of data types, please refer to the [API documentation on Data Types](https://docs.zaku.ai/en/latest/api/types.html).


<!-- prettier-ignore-start -->

```{eval-rst}
.. toctree::
   :hidden:
   :maxdepth: 1
   :titlesonly:

   Quick Start <quick_start>
   Report Issues <https://github.com/geyang/zaku/issues?q=is:issue+is:closed>
   CHANGE LOG <CHANGE_LOG.md>
   
.. toctree::
   :maxdepth: 3
   :caption: Tutorials
   :hidden:
   
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
   
   zaku TaskQ Client <api/zaku.md>
   zaku.base <api/base.md>
   zaku.interfaces — Type Interafce <api/interfaces.md>
   zaku.server — JobServer <api/server.md>
    
```


# Zaku Example

This example shows you how upload and download data


```python
from zaku import TaskQ

queue = TaskQ()
```
```
{'uri': 'http://localhost:9000', 'name': 'jq-49ae02fe-005f-482b-bac0-7bdac694566a', 'ttl': 5.0, 'no_init': None, 'verbose': None, 'ZAKU_USER': None, 'ZAKU_KEY': None}
```


Now we can add a few jobs to the queue

```python
queue.add({"seed": 100, "data": [1, 2, 3]})
```

If you want more control over the job id, you can specify it explicitly using the key argument:

```python
count = 0
queue.add({"seed": 200, "data": [1, 2, 3]}, key=count)
```
```python
job_id, job_config = queue.take()
```
```
69f40427-fcdd-4bfa-af2b-c2cb232dc333 {'seed': 100, 'data': [1, 2, 3]}
```


Now if we take again, it gives us a different one

```python
job_id, job_config = queue.take()
```
```
0 {'seed': 200, 'data': [1, 2, 3]}
```


Since we only added two jobs, the queue should be empty now. Let's check!

```python
result = queue.take()

doc.print(f"result is: [{result}]")
```

```
result is: [None]
```

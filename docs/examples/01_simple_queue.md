
# JobQ Example

This example shows you how upload and download data


```python
from jobq import JobQ

queue = JobQ()
```
```
{'host': 'http://localhost:9000', 'name': 'jq-1f936686-44c2-4e0f-8b94-f770bf5228ec', 'ttl': 5, 'no_init': None}
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
d4500aab-7793-431b-bee3-84c2d24455b5 {'seed': 100, 'data': [1, 2, 3]}
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

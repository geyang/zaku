
# JobQ Example

This example shows you how upload and download data


```python
from jobq import JobQ

queue = JobQ()
```
```
{'host': 'http://localhost:9000', 'name': 'jq-5f122009-9c53-4e62-83c4-8c935a544a4f', 'ttl': 5, 'no_init': None}
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
81eaae73-b4ec-4f47-be75-5e43c19aa505 {'seed': 100, 'data': [1, 2, 3]}
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

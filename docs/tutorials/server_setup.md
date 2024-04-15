# Setting Up a TaskServer

`Zaku` has two main components: a job queue server connected with a redis store, and a python client. 

You need to set up a `TaskServer` to use the queue. 

## Setting Up a Redis Store

First, you need to have a redis server running. You can install redis using the `redis-stack` docker distribution. This is because the `RedisJSON` module we use is not available in the default redis distribution. 

```shell
brew tap redis-stack/redis-stack
brew install redis-stack
```


```{admonition} Default `redis` cask does not work!
brew install redis
now running the server this way will not work

redis
```


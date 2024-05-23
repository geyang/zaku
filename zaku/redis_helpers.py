import redis


class ExceededRetriesError(Exception):
    pass


class RobustRedis(redis.asyncio.Redis):
    """Handles case when failover happened without loosing connection to old master"""

    async def execute_command(self, *args, **options):
        if not hasattr(self.connection_pool, "get_master_address"):
            return await super(RobustRedis, self).execute_command(*args, **options)

        _retry = 0
        _error = None

        # Since we have not encountered this during production, we will only
        # retry once, if it fails again, raise the exception.
        while _retry < 1:
            try:
                return await super(RobustRedis, self).execute_command(*args, **options)
            except redis.ConnectionError as err:
                _error = err

            except redis.ResponseError as err:
                _error = err

                if not err.message.startswith("READONLY"):
                    raise err

            finally:
                _retry += 1
                self.connection_pool.get_master_address()

        raise ExceededRetriesError("Exceeded retries due to" + _error)

    def pipeline(self, transaction=True, shard_hint=None):
        return SentinelAwarePipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)


class SentinelAwarePipeline(redis.asyncio.client.Pipeline):
    async def execute(self, raise_on_error=True):
        if not hasattr(self.connection_pool, "get_master_address"):
            return await super(SentinelAwarePipeline, self).execute(raise_on_error)

        _retry = 0
        _curr_stack = self.command_stack
        _error = None

        while _retry < 1:
            try:
                return await super(SentinelAwarePipeline, self).execute(raise_on_error)
            except redis.ConnectionError as err:
                _error = err
            except redis.ResponseError as err:
                _error = err
                if "READONLY" not in _error.message:
                    raise err

                if self.watching:
                    raise redis.WatchError("Sentinel failover occurred while watching one or more keys")

            finally:
                _retry += 1
                # restore all commands
                self.command_stack = _curr_stack
                self.connection_pool.get_master_address()
                self.reset()

        raise ExceededRetriesError("Exceeded retries due to" + _error)

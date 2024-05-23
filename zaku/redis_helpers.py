import redis
from redis import ResponseError, WatchError


class RobustRedis(redis.asyncio.Redis):
    """Handles case when failover happened without loosing connection to old master"""

    def execute_command(self, *args, **options):
        try:
            return super().execute_command(*args, **options)
        except ResponseError as e:
            if not e.message.startswith("READONLY"):
                raise
            old_master = self.connection_pool.master_address
            new_master = self.connection_pool.get_master_address()
            print("disconnecting")
            self.connection_pool.disconnect()
            return super().execute_command(*args, **options)

    def pipeline(self, transaction=True, shard_hint=None):
        return SentinelAwarePipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)


class SentinelAwarePipeline(redis.asyncio.client.Pipeline):
    def execute(self, raise_on_error=True):
        stack = self.command_stack
        try:
            return super(SentinelAwarePipeline, self).execute(raise_on_error)
        except ResponseError as e:
            if "READONLY" not in e.message:
                raise
            if self.watching:
                raise WatchError("Sentinel failover occurred while watching one or more keys")
            # restore all commands
            self.command_stack = stack
            old_master = self.connection_pool.master_address
            new_master = self.connection_pool.get_master_address()
            self.reset()
            self.connection_pool.disconnect()
            return super(SentinelAwarePipeline, self).execute(raise_on_error)

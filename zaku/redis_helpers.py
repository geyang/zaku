class SentinelAwareStrictRedis(StrictRedis):
    """ Handles case when failover happened without loosing connection to old master
    """
    def execute_command(self, *args, **options):
        try:
            return super(SentinelAwareStrictRedis, self).execute_command(*args, **options)
        except ResponseError as e:
            if not e.message.startswith("READONLY"):
                raise
            old_master = self.connection_pool.master_address
            new_master = self.connection_pool.get_master_address()
            self.connection_pool.disconnect()
            return super(SentinelAwareStrictRedis, self).execute_command(*args, **options)

    def pipeline(self, transaction=True, shard_hint=None):
        return SentinelAwareStrictPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )


class SentinelAwareStrictPipeline(StrictPipeline):
    def execute(self, raise_on_error=True):
        stack = self.command_stack
        try:
            return super(SentinelAwareStrictPipeline, self).execute(raise_on_error)
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
            return super(SentinelAwareStrictPipeline, self).execute(raise_on_error)

from zaku import TaskQ
from params_proto import Proto, ParamsProto


class Args(ParamsProto):
    uri: str = Proto(
        "http://localhost:9000", env="SAMPLER_URI", help="URI of the queue broker"
    )


queue = TaskQ(verbose=True)


"""
These are the unit tests for the type interfaces
"""
import numpy as np

from zaku.interfaces import Payload, ZData


def test_zdata_numpy():
    data = np.random.random([4, 8])
    packed = ZData.encode(data)
    unpacked = ZData.decode(packed)

    assert data.dtype == unpacked.dtype, "dtype is wrong."
    assert data.shape == unpacked.shape, "tensor shape is wrong."
    assert np.allclose(data, unpacked), "value should be close"


def test_zdata_torch():
    import torch

    data = torch.rand([5, 7])
    packed = ZData.encode(data)
    unpacked = ZData.decode(packed)

    assert data.dtype == unpacked.dtype, "dtype is wrong."
    assert data.shape == unpacked.shape, "tensor shape is wrong."
    assert torch.allclose(data, unpacked), "value should be close"


def test_payload_interface():
    payload = Payload(
        created_ts=0, status=None, grab_ts=0, value="hello", _greedy=False
    )
    msg = payload.serialize()
    job_new = Payload.deserialize(msg)
    # Need to change this
    print("job reconstructed", job_new)


def test_payload_greedy():
    import numpy as np
    import torch

    payload = Payload(
        np_tensor=np.array([10, 4]),
        torch_tensor=torch.Tensor([10, 4]),
    )
    msg = payload.serialize()
    job_new = Payload.deserialize(msg)

    assert np.allclose(
        payload.np_tensor, job_new["np_tensor"]
    ), "numpy tensor mismatches"
    assert np.allclose(
        payload.torch_tensor, job_new["torch_tensor"]
    ), "torch tensor mismatches"

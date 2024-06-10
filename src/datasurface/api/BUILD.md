# The RPC stubs must be built

This shows how to build the rpc stubs.

## Pre-requisites

```bash
pip install grpcio-tools
```

## Building the stubs

The python setup.py file has a build step for generating the stubs.

```bash
python setup.py build_py
```

This results in the api folder containing the generated stubs. There is an api_pb2 and api_pb2_grpc python file and pyi files for each.

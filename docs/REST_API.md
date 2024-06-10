# DataSurface REST APIs

DataSurface makes it easy to query various aspects of the Ecosystem model using REST API calls. It implements a stateless service which provides APIs to query the model producers, datastore schemas and so on. These APIs will be documented below:

## Read Only API

This API provides a read only interface to the associated model. All updates must be made through the normal github process. The API servers can be bounced and pointed at a newer branch to use for the model.

## Implementation

The APIs are implemented in python with code packaged in the normal datasurface package. The code is located in the datasurface/api/api.py module and its surrounding files. The FastAPI python package is used and pydantic is used to create JSON versions of objects being returned from the API.

This is packaged as a Docker container image which can be executed on a Kubernetes or similar compute cluster. It's envisages that even a single container instance is likely enough for the majority of DataSurface installations. Multiple can be used for fault tolerance. Fault tolerant instances are readily assembled using Kubernetes clusters with a simple router in front.

The Docker containers will clone the version of the model when they start up. They will load the model in to memory and service API calls using the in memory model. This is efficient and scales horizontally perfectly. It's also very easy to make geo redundant clusters if that is necessary. If the model changes then the containers need to be recycled and the new model cloned on restart.

The use of FastAPI and pydantic means there is a Swagger API and documentation set available online.

There is no central database behind these API servers. The model is simply loaded in to each servers memory when it starts and then queried from memory.

## Producer APIs

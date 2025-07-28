# DataSurface REST APIs

DataSurface makes it easy to query various aspects of the Ecosystem model using REST API calls. It implements a stateless service which provides APIs to query the model producers, datastore schemas and so on. These APIs will be documented below:

## Read Only API

This API provides a read only interface to the associated model. All updates must be made through the normal github process. The API servers can be bounced and pointed at a newer branch to use for the model.

## Implementation

This has been implemented as a simple REST API using FastAPI. It supposed a series of commands that can be used to query the model. The commands will return json objects representing the data requested.

There is no central database behind these API servers. The model is simply loaded in to each servers memory when it starts and then queried from memory.

## Producer APIs

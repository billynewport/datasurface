# DataSurface GRPC API Service

This code exposes a GRPC API to allow the model to be accessed by non Python Code. This was first developed to allow Java Spark Jobs to retrieve model elements
needed for their execution. The code loads an ecosystem model from the filesystem and then serves the model through an API to clients.

## Running the GRPC API

The service is packaged as a Docker container. The Docker container should be started with the model to be served cloned from the repository and available to the Docker container
on a filesystem mount point.

When the model changes through a commit then the model can be cloned again and the container restarted to serve the new model. This can be done behind a load balancer to allow continuous
operation of the service when the model changes. Model changes are always backwards compatible.
# Data surface, a data ecosystem broker

The goal is this project is to eliminate data pipelines as a manual task within an enterprise. Enterprise developers should be performing only the following tasks:

* Producing new data

* Consuming existing data to extract value

* Producing derivative value added data from existing data

Nobody should be writing data pipeline code. The data ecosystem should be able to infer the data pipelines from the above metadata describing the data ecosystem. This metadata is a model of the data ecosystem. The model is stored in a git repository and is the official version of truth for the data ecosystem. The model is used to create and maintain data pipelines. The model is also used to provide governance and control over the data within the enterprise.

This model has contributions from ecosystem managers, data governors, data producers, data consumers, data transformers and data platforms. Together, a new element, the Ecosystem broker, interprets the intentions of these actors and creates the data pipelines needed to support the intentions in the model as well as makes sure these pipelines do not violate the governance policies of the enterprise.

This model is expressed using a Python DSL and is stored in a github repository which serves as the official version of truth for the model. Authorization for modifying different elements of the model is federated. Each region of the model can be assigned to be modifiable using only pull requests from a specific github repository.

The main repository uses github action handler to make sure the central model stays self consistent, backwards compatible and consistent with the governance policies of the enterprise.

Data platforms can then use the model to create and maintain data pipelines. Data platforms will usually translate the data pipeline graph described by the model in to a set of pipelines which are created by translating the model pipeline in to IaC artifacts which are provisioned on the target infrastructure platforms.

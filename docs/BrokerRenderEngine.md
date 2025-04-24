# Broker Render Engines

The user will define a datasurface Ecosystem in a repository. This specifies the user intentions in terms of what data is available, who needs what data in what form and context, the dependencies and so on. It also defines the available DataPlatforms that can be used to render these intentions in to working data ecosystems.

The BrokerRenderEngine is responsible for providing the compute, storage and credential management for DataPlatforms to execute within. Examples of BrokerRenderEngines are:

* A local Docker Swarm cluster. See [SwarmRenderer.md](SwarmRenderer.md)
* An AWS ECS cluster
* A Kubernetes cluster
* A Google Cloud Run cluster

Users will need to make sure all credentials defined in the model are also defined in the CredentialStore configured for the active BrokerRenderEngines. This is checked when the merge handler executes. The broker render engine also is used when linting the pipeline graphs in git actions when pull requests are being approved. Some things can only be checked when running in the execution environment rather than the git environment. These are different.

Operations staff will receive the validation errors from the MERGE handler rather than the development teams who will see the errors caused by the model itself.

BrokerRenderEngine implementations are provided as part of the Ecosystem definition in the model and managed by the Ecosystem team. They will decide which implementations are appropriate for the various Workspaces. Linting the BrokerRenderEngine may also fail on unsupported DataPlatform instances for example.

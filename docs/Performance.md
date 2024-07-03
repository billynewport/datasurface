# DataSurface Performance

Here, we try to identify the performance aspects of DataSurface. We will consider the performance of the DataSurface model validation, the CI/CD speeds, the DataPlatform performance and the DataPlatform monitoring.

## DataSurface Model Validation speeds

The first performance aspect to consider is how quickly can developers who make changes to the ecosystem model work with and check in changes to their pieces of the model. As the model gets larger in size, does model validation and checking slow down for example. We expect there to be models with 10K data producers, 100k datasets, 10k data transformers and 3k data consumers. We expect the model to be able to handle this size of model and larger.

## DataSurface CI/CD speeds

Do developers need to wait for their pull requests to be validated/approved and merged in to the main branch. Once there, how long does it take DataSurface to render the changes resulting from that pull request.

Render the changes means, providing revised data graphs representing the data movement to the existing DataPlatforms selected for a section of the total model. How long will it take a particular DataPlatform to reconcile the current reality of the data pipelines with those in the newly changed model. Most DataPlatforms will use IaC techniques to make these changes. The graph provided from DataSurface will be changed in to a terraform or cloudformation script and then applied to Amazon AWS or Microsoft Azure infrastructure. The terraform or cloudformation provider will take time to do this reconciliation and it will also take time for infrastructure to be provisioned when required by the changes.

## DataPlatform performance

The last factor to consider is DataPlatform performance. DataPlatforms are responsible for providing data supplied by data producers to the Workspaces owned by consumers. Consumer specify their requirements in terms of data latency and other factors. The DataPlatform is chosen by DataSurface for a set of consumers with common needs to best fulfill those needs.

DataPlatforms have 2 performance areas.

### Infrastructure and data provisioning times

The first is infrastructure provisioning times as well as data population times for that infrastructure. For example, it may take Amazon AWS a minute or two to provision a new S3 bucket in the west coast region. However, if the consumer using that bucket asked for a PB of data in their Workspace, it could even take days to push that data from an EU based data producer to the west coast S3 bucket. DataPlatforms will need to provide warnings to users if the infrastructure provisioning and data population times are going to be long or result in high costs that are not anticipated.

### Data Container Performance

Data containers are SQL databases, Lakehouses and so on. The performance of these is dependent on these. DataPlatforms will typically choose a set of Data containers to host data and compute for data pipelines that it builds. When the broker chooses a data platform for a consumer, query latency requirements are taken in to account. However, query requirements is very application specific. So, it's likely there will be broad classes of performance that consumers will fit in to. A DataSurface system will support a range of data containers and Data platforms will support subsets of those data containers. Data container performance is typically managed through appropriate sizing of the infrastructure hosting the containers, indexing/partitioning and horizontally scaling out either computer and/or storage as load increases.

### Data latency, throughputs, operational costs

The next area is does the DataPlatform meet the indicated latency desires of all the consumers using Workspaces using that DataPlatform. Are there throughput limitations? These may be unavoidable due to the nature of the data or infrastructure limits. The DataPlatform should be transparent to allow consumers to see where issues are. The DataPlatform should also be able to provide cost estimates for the data movement and storage. This is important for the consumers to know if they are going to be able to afford the data they are asking for. They may change the requirements if they know the costs are going to be too high. This may also result in a new DataPlatform being developed to meet the needs of the consumers unhappy with the current status quo. Consumers may even decided to use a different cloud vendor and have Workspaces hosted there if its cheaper.

It is also possible that an enterprise is operating under a fixed budget within which a set of consumers must operate. This may result in a lower than expected performance for a given consumer while meeting the enterprise budget. This is a trade off that the enterprise must make and DataSurface should be able to provide the information to make that decision and then enforce it.

## DataPlatform monitoring

Ideally, DataPlatforms will send operational metrics back to a central service which allows DataSurface consumers to observe the operational state behind the Workspaces they are using.

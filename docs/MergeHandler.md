# Merge Handler

This is code which executes after changes are merged in to the live version of the DataSurface model. It will execute within the compute context of a BrokerRenderEngine.

The MergeHandler will:

* Clone the repository to the local file system
* Load and validate the model into memory.
* Calculate the intention graphs for each DataPlatform
* Validate the everything is configured correctly with respect to credentials and DataPlatforms
* Instruct DataPlatforms to render their subsets of the intention graph in to an executable data pipeline implementation.

The operation team will need to monitor its execution as there can be errors from DataPlatforms or the merge handler itself.

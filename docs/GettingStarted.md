# Getting Started

## Setup the main github repository and its workflows

The main github repository is the source of truth for the data ecosystem. It contains the model of the data ecosystem. The model is used to create and maintain data pipelines. The model is also used to provide governance and control over the data within the enterprise. We need to install the github action handler to make sure the central model stays self consistent, backwards compatible and consistent with the governance policies of the enterprise.

[See this document for an overview of how DataSurface uses Github](HowGitHubIsUsed.md)

## Define the top level ecosystem, infrastructurevendors, dataplaforms and goverance zones

The ecosystem is the top of the model. It defines the infrastructure vendors that the ecosystem can work with. Each vendor also defines the locations they provide to host data and/or compute. The ecosystem also defines the data platforms that can be used to fulfill the data movement requirements of consumers of data within the ecosystem.

Next, governance zones can be declared by adding a GovernanceZoneDeclaration for each governance zone. This defines the name of the zone as well as the github repository/branch which is used for authoring the governance zone. Only the main ecosystem repository can be used to declare a zone. The governance zone cannot be deleted until the governance zone github repo removes references to the governance zone.

For more information on different aspects of the system see the following documents:

* [Ecosystem](Ecosystem.md)
* [Governance Zones](GovernanceZone.md)
* [Dataplatforms](DataPlatform.md)
* [Teams](Teams.md)
* [How github is used by Datasurface](HowGitHubIsUsed.md)
* [Datastores](Datastores.md)
* [Workspaces](Workspaces.md)
* [Data Transformers](DataTransformer.md)
* [Data Containers](DataContainers.md)
* [Data Classification System](DataClassification.md)

## Commands to make a set of changes

Different parts of the ecosystem model are own by different github repositories. This allows for the federation of the authority to make edits to different parts of the model. The live or production version of the model is in the main ecosystem repository. When a team wishes to change their part of the model. They should fork the main to their github repository (the one responsible for their part of the model). Then they can make changes to their part of the model. Once they are happy with the changes, they can make a pull request to the main ecosystem repository. The github action handler will then be triggered to make sure the changes are self consistent, backwards compatible and consistent with the governance policies of the enterprise. Any issues will be reported back to the pull request.

```shell
git clone https://www.github.com/billynewport/datasurfacetemplate
cd datasurfacetemplate
git checkout main
git checkout -b edits
```

Now, make some changes to the model. For example, add a new data platform. Next, commit the changes and push them to the edits branch.

```shell
git add .
git commit -m "Some edits"
git push origin edits
```

Now, we need to make a pull request for these changes from the edits branch to the main branch. Once the pull request is approved, the changes will be merged into the main branch. The github action handler will then be triggered to make sure the changes are self consistent, backwards compatible and consistent with the governance policies of the enterprise. Any issues will be reported back to the pull request.

## How to find errors in your pull request

The pull request logs will have the errors and warnings. Even if the pull succeeds, it's still a good idea to look for warnings. Here is an example showing different errors. This log shows when the wrong repository is used to change an object.

```Logs
Ecosystem(Test)
   ProblemSeverity.ERROR:Unauthorized defaultDataPlatformn change: None is different from AzureDataplatform(Azure Platform)
   ProblemSeverity.ERROR:Ecosystem(Test) top level owned by GitRepository(billynewport/datasurfacetemplate/main) has been modified by an unauthorized source GitRepository(billynewport/datasurfacetemplate/edits)
   AuthorizedObjectManager(zones)
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(UK) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(EU) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(USA) has been added
   Vendors
     ProblemSeverity.ERROR:InfrastructureVendor(Azure, CloudVendor.AZURE) has been added
     ProblemSeverity.ERROR:InfrastructureVendor(AWS, CloudVendor.AWS) has been added
   DataPlatforms
     ProblemSeverity.ERROR:AmazonAWSDataPlatform(AWS Platform) has been added
     ProblemSeverity.ERROR:AzureDataplatform(Azure Platform) has been added
   AuthorizedObjectManager(zones)
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(UK) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(EU) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(USA) has been added
     ProblemSeverity.ERROR:AuthorizedObjectManager(zones) top level owned by GitRepository(billynewport/datasurfacetemplate/main) has been modified by an unauthorized source GitRepository(billynewport/datasurfacetemplate/edits)
 Ecosystem(Test)
   ProblemSeverity.ERROR:Unauthorized defaultDataPlatformn change: None is different from AzureDataplatform(Azure Platform)
   ProblemSeverity.ERROR:Ecosystem(Test) top level owned by GitRepository(billynewport/datasurfacetemplate/main) has been modified by an unauthorized source GitRepository(billynewport/datasurfacetemplate/edits)
   AuthorizedObjectManager(zones)
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(UK) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(EU) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(USA) has been added
   Vendors
     ProblemSeverity.ERROR:InfrastructureVendor(Azure, CloudVendor.AZURE) has been added
     ProblemSeverity.ERROR:InfrastructureVendor(AWS, CloudVendor.AWS) has been added
   DataPlatforms
     ProblemSeverity.ERROR:AmazonAWSDataPlatform(AWS Platform) has been added
     ProblemSeverity.ERROR:AzureDataplatform(Azure Platform) has been added
   AuthorizedObjectManager(zones)
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(UK) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(EU) has been added
     ProblemSeverity.ERROR:GovernanceZoneDeclaration(USA) has been added
     ProblemSeverity.ERROR:AuthorizedObjectManager(zones) top level owned by GitRepository(billynewport/datasurfacetemplate/main) has been modified by an unauthorized source GitRepository(billynewport/datasurfacetemplate/edits)
```

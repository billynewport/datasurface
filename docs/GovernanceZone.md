# Governance Zone

A governance zone is used to define a region of the data ecosystem. It constrains the data platforms, vendors and locations that can be used within a zone. For example, cloud vendors can be banned OR mandated for all data stores and consumers within a zone. The allowable data classification can also be restricted. A zone can be defined which can hold all data classifications/privacy levels but that bans all cloud vendors and mandates data is kept within the US for example. Another zone can be defined which allows cloud vendors but disallows PC1,PC2,PC3 data.

## Declaring a GovernanceZone

The ecosystem repository must declare the governance zone initially and indicate which repository will be used to make changes to the zone and its contents. Once this is committed then the governance zone can be used to specify the zone metadata. The zone repository should clone the main repository, make changes, commit them and then issue a pull request to make the changes in the main repository. The pull request will be vetted by the main repository and if it passes the vetting then the changes will be merged in to the main repository. At this point, the changes are live. If further changes are required to the zone then repeat this procedure.

## Deleting a governance zone

All governance zone metadata should be removed from the zone repository and merged with the main repository. Once the governance zone is not longer defined at all then the zone declaration can be removed using the ecosystem repository.

If any other zones are using data stores/data sets from the zone to be deleted then they too must be deleted or modified to remove the references to the zone to be deleted. This is to ensure that the model remains consistent.

## Limiting the infrastructure vendors available within a zone

A governance zone can specify zero or more InfrastructureVendorPolicy objects. These can filter the list of vendors allowed to be used on this zones data. An individual policy can either disallow a set of vendors or allow a set of vendors.

## Limiting the infrasturcture vendor locations available within a zone

A zone can specify zero of more InfrastructureLocationPolicy objects. These can filter the list of locations allowed to host assets where data policied by this zone can be stored. Data could be restricted to the EU or the USA.

## Limiting the data platforms available within a zone

A zone can also restrict the choices of dataplatforms available to the ecosystem for servicing the clients of its data. A dataplatform can ONLY use data that is allowed by the datasets governancezone.

## Limiting the data classifications available within a zone

A Governance Zone can be setup to only host data of specific classifications. This can be useful for setting up a restricted zone which holds sensitive data. That zone can restrict the data to enterprise private data centers for example. This prevents restricted data from being used in the cloud.

Another zone could be setup for data which has been cleared for use on the cloud and enterprise private data centers.

## Declaring Teams within a zone

A TeamDeclaration can be added to a zone to declare a team. Every team in a zone first requires a TeamDeclation to be added for it by the governance zone github repository. Once the declaration is present in the main branch, this declares the team as well as the github repository/branch which is used for authoring the team. The team cannot be deleted until the team github repo removes references to the team.

The team git repository can now be used in the normal way to define the team objects, the datastores, datasets and workspaces.
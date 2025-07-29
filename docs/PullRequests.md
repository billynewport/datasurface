# Pull request vetting

The ecosystem repository is the primary source of truth for everyone. GovernanceZones and Teams have their own repositories which are used to enforce who can edit what metadata.

## Making changes at the ecosystem level

These changes can be made directly against the ecosystem git repository. However, only top level changes can be made. The changes cannot leave the model inconsistent. For example, a GovernanceZone cannot be deleted until the GovernanceZone definition is first removed by the GovernanceZone team using their git repository.

## Making changes at the GovernanceZone level

The GovernanceZone git repository manages platforms, teams and policies. The governance zone cannot change team level objects. Only the responsible teams can do that through the team repositories. Again, a Governance zone cannot delete a team until the team definition is first removed by the team using the teams git repository.

## Making changes at the Team level

The team git repository manages the team and its associated child objects such as datastores, datasets and workspaces. These objects can be changed but the model must remain consistent or else the changes will be rejected. For example, a dataset can be deleted but if a workspace exists that references the dataset then the delete will be rejected. The workspace must be deleted first. The workspace can be owned by a different team. The workspace team would need to remove the dataset reference from the work space and make that change in the ecosystem repository before the dataset can be deleted by the dataset team. This interlocking of repositories ensures that the model is always consistent.

## Mechanics of making changes

The repository should be a fork from the ecosystem live repository. The repository should be refreshed from the live branch before starting. The changes can then be made, committed and a pull request to the ecosystem repository submitted. The pull request will have a set of automatic validations run against it and humans may inspect it also before it can be merged in to the eco system repository.

## Automatic validations

- Are the changes coming from the correct repository?
    Ecosystem, GovernanceZone and team can all be associated with different git repositories. Model changes must come from the appropriate repository. Changes should not be made directly to the live repository as no validation will be run against them.
- Does the model pass lint tests?
    Lint checks verify the attributes are appropriately named, references between objects are valid and generally that the model is self-consistent. Primary key columns cannot be nullable. Workspaces cannot reference a deprecated dataset unless its allows deprecated datasets in its definition.
- Is the new model backwards compatible with the existing model?
    The new model must be backwards compatible with the old model. Dataset schemas can add new nullable columns but columns cannot be deleted for example.
- Do all the assigned DataPlatforms accept their Workspaces and supporting data pipelines. This can fail because of platform limitations, unsupported database or ingestion types for example.
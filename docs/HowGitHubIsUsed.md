# How Data surface leverages GitHub

Each datasurface defines a single ecosystem. This ecosystem resides as a Python DSL within a single github repository on a main branch. This is the live version of the ecosystem. The ecosystem consists of several parts, Governance Zones and Teams. Each part is associated with a single github repository and branch. These can be the same repository as the main ecosystem repository or different repositories. Indeed, the simplest setup is actually to use the same repository and branch with the ecosystem and the parts. This setup is simplest but it does not allow for changes to the parts to be managed by different teams of people. It's recommended to use a different branch and/or repository with every part. Thus each GovernanceZone would have its own branch/repository. Each Team would also have its own branch/repository. This gives maximumal control of who can change the different parts. No matter how this is setup, the branch/repository used with the ecosystem is ALWAYS the live version of the ecosystem.

## How the ecosystem metadata is changed

This can be committed directly to the ecosystem branch/repository.

## How other parts are modified

The part repository should clone the live ecosystem. The part repository can then make changes to the part metadata and commit them. A pull request against the ecosystem repo live branch can then be created. If this pull request is approved then the part changes are live.

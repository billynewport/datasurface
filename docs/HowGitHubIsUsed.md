# How Data surface leverages GitHub

## Data catalogs the old way

I have built this kind of data catalog before. I used a traditional database to store the metadata like HBase or pick your own favorite database. I defined the schema, implemented APIs to create, read, update and delete this data. The CRUD services. I then built versioning in to it so that we could see the history of the metadata. I implemented workflows with various approvals for some of the CUD services. I stored the output of these workflows in an auditing database.

I built a GUI on top of this database. The GUI worked well for simple things. It did not work well for teams managing many things. I added terraform hoping that it would make managing the artifacts for large teams easier but terraform, and I apologize up front, is ugly. I'm not the only one, most vendors are implementing DSLs on top to make it better. Microsoft implemented bicep on top of ARM for the same purpose.

So, having done this before, I did not want to do it again. Integrating this type of catalog in to an enterprise would be very difficult. Every enterprise would want different workflows, approvals, auditing and so on. Instead, I think it would be better to use the worlds most popular metadata repository, github. Github can store metadata in the form of objects described in files. It has versioning of course, it has workflows, authorizations, authentications, reporting and so on. Why build all this again?

It's easy to geographically replicate the repository, clone it on a local filesystem in any country you want. Github solves a lot of the problems I solved previously using proprietary approaches and arguably does it better in many cases.

Datasurface tries to leverage github as the main model repository in what I hope is a natural way. It works with the normal github 

Each datasurface defines a single ecosystem. This ecosystem resides as a Python DSL within a single github repository on a main branch. This is the live or production version of the ecosystem. The ecosystem consists of several parts, the ecosystem, the Governance Zones and Teams. Each part is associated with a single github repository and branch. These can be the same repository as the main ecosystem repository or different repositories. Indeed, the simplest setup is actually to use the same repository and branch with the ecosystem and the parts. This setup is simplest but it does not allow for changes to the parts to be managed by different teams of people. It's recommended to use a different branch and/or repository with every part. Thus each GovernanceZone would have its own branch/repository. Each Team would also have its own branch/repository. This gives maximumal control of who can change the different parts. No matter how this is setup, the branch/repository used with the ecosystem is ALWAYS the live version of the ecosystem.

## How the ecosystem metadata is changed

This can be committed directly to the ecosystem branch/repository.

## How other parts are modified

The part repository should clone the live ecosystem. The part repository can then make changes to the part metadata and commit them. A pull request against the ecosystem repo live branch can then be created. If this pull request is approved then the part changes are live.

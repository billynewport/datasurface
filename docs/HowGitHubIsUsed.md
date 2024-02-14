# How Data surface leverages GitHub

## Data catalogs the old way

I have built this kind of data catalog before. I used a traditional database to store the metadata like HBase or pick your own favorite database. I defined the schema, implemented APIs to create, read, update and delete this data. The CRUD services. I then built versioning in to it so that we could see the history of the metadata. I implemented authentication and authorization. Workflows with various approvals for some of the CUD services. I stored the output of these workflows in an auditing database.

I built a GUI on top of this database. The GUI worked well for simple things. It did not work well for teams managing complex projects. I added terraform hoping that it would make managing the artifacts for large teams easier but terraform, and I apologize up front, is ugly. I'm not the only one, most vendors are implementing DSLs on top to make it better. Microsoft implemented bicep on top of ARM(terraform look alike) for the same purpose.

So, having done this before, I did not want to do it again. Integrating this type of catalog in to an enterprise would be very difficult. Every enterprise would want different workflows, approvals, auditing and so on. Instead, I think it would be better to use the worlds most popular metadata repository, github. Github can store metadata in the form of objects described in files. It has versioning of course, it has workflows, authorizations, authentications, reporting and so on. Why build all this again?

It's easy to geographically replicate the repository, clone it on a local filesystem in any country you want. Github solves a lot of the problems I solved previously using proprietary approaches and arguably does it better in many cases.

Datasurface tries to leverage github as the main model repository in what I hope is a natural way. It works with the normal github mechanisms. The model is stored in a main branch in a repository. The customer can setup authorizations on that repository to limit who can modify it directly, these are admin staff in our model.

Each datasurface defines a single ecosystem model. This ecosystem resides as a Python DSL within a single github repository on a main branch. This is the live or production version of the ecosystem. The ecosystem consists of several parts, the ecosystem, the Governance Zones and Teams. Each of these parts is associated with a single github repository and branch. These can be the same repository as the main ecosystem repository or different repositories. Indeed, the simplest setup is actually to use the same repository and branch with the ecosystem and the parts. This setup is simplest but it does not allow for changes to the parts to be managed by different teams of people. It's recommended to use a different branch and/or repository with every part. Thus each GovernanceZone would have its own branch/repository. Each Team would also have its own branch/repository. This gives maximal control of who can change the different parts. Authorizations can be different for each part if each part uses its own repository. No matter how this is setup, the branch/repository used with the ecosystem is ALWAYS the live version of the ecosystem.

## How the ecosystem metadata is changed

This can be committed directly to the ecosystem branch/repository by people authorized to make changes on the main repository. The ecosystem team defines the parameters for the ecosystem. The Data platforms available, the vendors and locations, the governance zones that are available to be defined.

## How other parts are modified

The part repository should clone the live ecosystem. The part repository can then make changes to the part metadata and commit them. A pull request against the ecosystem repo live branch can then be created. If this pull request is approved then the part changes are live.

## Validations are model based, not file based

It's important to realize that besides the ```eco.py``` file, datasurface imposes no restrictions on what files are called or how the model is organized. When it validates an incoming pull request, it simply loads the incoming model using the createEcosystem method in eco.py, it loads the existing model similarly and then it compares the models. How the models are constructed is not relevant. Authorization checks on made based on which parts of the model were changed by the pull request git repository.

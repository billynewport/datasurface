# How Data surface leverages CI/CD capable repositories

## This is not GitHub specific

First thing to realize is that DataSurface will work with any CI/CD capable repository with minimal work. It currently provides a GitHub specific plugin but it's trivial to add more as needed.

## Data catalogs the old way

We have built this kind of data catalog before. We used a traditional database to store the metadata like HBase or pick your own favorite database. We defined the schema, implemented APIs to create, read, update and delete this data. The CRUD services. We then built versioning in to it so that we could see the history of the metadata. We implemented authentication and authorization. Workflows with various approvals for some of the CUD services. We stored the output of these workflows in an auditing database.

A GUI was built on top of this database. The GUI worked well for simple things. It did not work well for teams managing complex projects. So, a terraform API was added hoping that it would make managing the artifacts for large teams easier but terraform, and I apologize up front, is ugly. There are things YAML is good at but defining complex objects and models isn't one of them. I'm not the only one, most vendors are implementing DSLs on top to make it better. Microsoft implemented bicep on top of ARM(terraform look alike) for the same purpose.

## Data catalog stored as a Python DSL in CI/CD versioned repositories like GitHub

So, having done this before, we did not want to do it again. Integrating this type of catalog in to an enterprise would be very difficult. Every enterprise would want different workflows, approvals, auditing and so on.

Metadata is todays world is managed by CI/CD systems with versioned repositories. GitHub is the biggest example. Github can store metadata in the form of objects described in files. It has versioning of course, auditing, it has workflows, authorizations, authentications, plugins, reporting and so on. Why build all this again?

It's easy to geographically replicate the repository, clone it on a local filesystem in any country you want. Github solves a lot of the problems we solved previously using proprietary approaches and arguably does it better in many cases.

Datasurface tries to leverage github as the main model repository in what we hope is a natural way. It works with the normal github mechanisms. The model is stored in a main branch in a repository. The customer can setup authorizations on that repository to limit who can modify it directly, these are admin staff in our model.

Each datasurface defines a single ecosystem model. This ecosystem resides as a Python DSL within a single github repository on a main branch. The repository has a single well known module in the root folder called ```eco.py```. This module must have a well known function with the following signature:

```python
def createEcosystem() -> Ecosystem:
```

Datasurface calls this function and receives your Ecosystem model as the return value. This is the live or production version of the ecosystem. The ecosystem model consists of several parts, the ecosystem, the Governance Zones and Teams. Each of these parts is associated with a single github repository and branch. These can be the same repository as the main ecosystem repository or different repositories. Indeed, the simplest setup is actually to use the same repository and branch with the ecosystem and the parts. This setup is simplest but it does not allow for changes to the parts to be managed by different teams of people. It's recommended to use a different branch and/or repository with every part. Thus each GovernanceZone would have its own branch/repository. Each Team would also have its own branch/repository. This gives maximal control of who can change the different parts. Authorizations can be different for each part if each part uses its own repository. No matter how this is setup, the branch/repository used with the ecosystem is ALWAYS the live version of the ecosystem.

## How the ecosystem metadata is changed

This can be committed directly to the ecosystem branch/repository by people authorized to make changes on the main repository. The ecosystem team defines the parameters for the ecosystem. The Data platforms available, the vendors and locations, the governance zones that are available to be defined.

## How other parts are modified

The part repositories should clone the live ecosystem. The part repository can then make changes to the part metadata and commit them. A pull request against the ecosystem repo live branch can then be created. If this pull request is approved then the part changes are live.

## Validations are model based, not file based

It's important to realize that besides the ```eco.py``` file, datasurface imposes no restrictions on what files are called or how the model is organized. When it validates an incoming pull request, it simply loads the incoming model using the createEcosystem method in eco.py, it loads the existing model similarly and then it compares the models. How the models are constructed is not relevant. Authorization checks on made based on which parts of the model were changed by the pull request git repository.

Thus, the ecosystem team will define the initial ```eco.py``` file which initializes an Ecosystem with maybe a single GovernanceZone like this:

```python

# Define a test Ecosystem
def createEcosystem() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitHubRepository("owner/surfacerepo", "main"),
        # Declare the Azure Data Platform and make it the default
        DefaultDataPlatform(AzureDataplatform("Azure Platform", AzureKeyVaultCredential("vault", "maincred"))),
                              
        GovernanceZoneDeclaration("Azure_USA", GitHubRepository("owner/azure_usa", "main")),

        InfrastructureVendor("Azure",
            PlainTextDocumentation("Microsoft Azure"),
                InfrastructureLocation("East US") # Virginia
            )
        )
    return e

```

Next, the Azure_USA zone team will clone the main repository to the zones one (owner/azure_usa#main) and then edit it like this to declare some teams:

``` python

# Define a test Ecosystem
def createEcosystem() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitHubRepository("owner/surfacerepo", "main"),
        # Declare the Azure Data Platform and make it the default
        DefaultDataPlatform(AzureDataplatform("Azure Platform", AzureKeyVaultCredential("vault", "maincred"))),
                              
        GovernanceZoneDeclaration("Azure_USA", GitHubRepository("owner/azure_usa", "main")),

        InfrastructureVendor("Azure",
            PlainTextDocumentation("Microsoft Azure"),
                InfrastructureLocation("East US") # Virginia
            )
        )

    gzUSA : GovernanceZone = e.getZoneOrThrow("Azure_USA")

    gzUSA.add(
        TeamDeclaration("US_Team", GitRepository("owner/us_team", "main"))
    )
    return e

```

This is then committed to azure_usa and then a pull request is made against the ecosystem repository. The pull request is validated by workflows and git action handlers before it is allowed to be committed in the ecosystem repository. Now, the US team can define the Team object and create some Datastores representing data they own that can be used by others and possibly some Workspaces for their own data consumption needs.

It's important to realize, the zone repo just added code to eco.py to extend the model to include a governance zone declaration. Similarly, the us team will add lines to flesh out the team object with data producers and consumers. Datasurface doesn't care about how the model is constructed. You can have a file per governance zone or per team or keep it all in one file. It's up to you and what makes maintaining the model easiest. Datasurface just sees the Ecosystem object returned by the createEcosystem function.

## What are the checks for merging a pull request in to the main Ecosystem repository

When a pull request is made OR a commit is made directly against the main Ecosystem repository then the incoming model is checked against the existing model. First, the incoming model is checks that it's self consistent and contains no errors. This checks for things like bad references, identifiers which don't meet lexical rules and so on.

Next, the incoming model is checked that the pieces of the incoming model which are different from the existing main model, can be changed using the pull request repository. This ensures that only the correct repos can change the Ecosystem, the various governance zones and the various teams.

Finally, the incoming model is checked that its backwards compatible with the existing model. This means that the incoming model can add new things but it cannot remove things or change things in a way that would break the existing model. For example, a dataset can add new columns but it cannot remove columns. It can also change column types to compatible types, widening strings for example, widing numeric types and so on.

If all 3 of these checks pass then the incoming change is allowed to merge with the main Ecosystem repository. If any of these checks fail then the pull request is rejected with comments indicating whats wrong.

## Worked example tutorial

You can find a tutorial on getting started [here](Tutorial.md)

## What about other CI/CD repositories?

DataSurface can work with any version control system with a CI/CD capability.

There is nothing specific to GitHub in DataSurface. DataSurface comes with an implemented interface for GitHub but it's easy to add another interface for another repository. The model is independent of the repository. The model is the same whether it's used with GitHub or another repository. Please see [here](HowToReplaceGithubAsTheRepository.md) for more information on using DataSurface with other CI/CD repositories besides GitHub.

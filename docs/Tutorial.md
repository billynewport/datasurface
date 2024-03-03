# Creating the initial ecosystem model

DataSurface has a starter repository which can be cloned to get started. It's located [here](http://www.github.com/billynewport/datasurfacetemplate). To get started, we need to make a new github repository which will contain the live version of our ecosystem model.

The is named http://www.github.com/billynewport/test-surface in this tutorial. The main branch will be the live model. We will create the test-surface repository in github. We now have an empty repository. Next, we will clone the starter repository and then push it to the new repository.

```shell
mkdir ~/models
mkdir ~/models/AcmeEcoMain
cd ~/models/AcmeEcoMain
git clone https://www.github.com/billynewport/datasurfacetemplate
```

This clones the repository in to the datasurfacetemplate folder in the current directory (~/models/AcmeEcoMain). Next, we need to edit the eco.py file to change the main repository for the model to this repository.

```python
from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository


def createEcosystem() -> Ecosystem:
    return Ecosystem(
        "AcmeEco",
        GitHubRepository("billynewport/test-surface", "eco_edits")
        )
```

We will make all ecosystem level edits using the eco_edits branch and use pull requests to push the changes to the main branch. This is a good practice to ensure that the changes are self consistent, backwards compatible and consistent with the governance policies of the enterprise. Any issues will be reported back to the pull request.

```shell
git add .
git commit -m "Modified the eco model repo to point to our repository"
git remote set-url origin https://www.github.com/billynewport/test-surface.git
git push origin main
```

At this point, we have the main branch of the test-surface repository initialize with a brand new empty ecosystem model which allows changes via pull requests from the eco_edits branch of the same repository.

## Adding a vendor and some locations

An Ecosystem has storage and compute infrastructure which can be provided by many vendors using any locations those vendors support. We will add an Azure vendor with two locations, East US and West US.

```python
from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import CloudVendor, InfrastructureLocation, InfrastructureVendor
from datasurface.md.Documentation import PlainTextDocumentation


def createEcosystem() -> Ecosystem:
    return Ecosystem(
        "AcmeEco",
        GitHubRepository("billynewport/test-surface", "eco_edits"),
        InfrastructureVendor(
            "Azure",
            CloudVendor.AZURE,
            PlainTextDocumentation("Microsoft Azure"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation(
                    "Central",
                    InfrastructureLocation("Central US"),  # Iowa
                    InfrastructureLocation("North Central US"),  # Illinois
                    InfrastructureLocation("South Central US"),  # Texas
                    InfrastructureLocation("West Central US")),  # Wyoming
                InfrastructureLocation(
                    "East",
                    InfrastructureLocation("East US"),  # Virginia
                    InfrastructureLocation("East US 2"),  # Virginia
                    InfrastructureLocation("East US 3")),  # Georgia
                InfrastructureLocation(
                    "West",
                    InfrastructureLocation("West US"),  # California
                    InfrastructureLocation("West US 2"),  # Washington
                    InfrastructureLocation("West US 3")))  # Arizona
            )
        )
```

This defines a vendor called Azure with 3 geographic regions Central, East and West. These regions contain the vendor locations. The leaf infralocations should be named exactly after the vendors locations. The region location names are model names and present for our information only. We can now check in these changes from the eco_edits branch to the main branch.

```shell
# Use your favorite editor to make the above changes to eco.py
git add .
git commit -m "Added Azure vendor and locations"
git checkout -b eco_edits
git push origin eco_edits
```

Next, head to github.com and make a pull request from the eco_edits branch to the main branch. The github checks will run that were added from the starter project. These check that there are no syntax errors in the eco.py file, that the pull request only contains .py files and that the changes are valid/self consistent and are compatible with the existing model. If these checks fail then you can perform the following steps to fix the issues so that the checks will succeed.

```shell
# Edit the eco.py file and fix the issues
git add .
git commit -m "Fixed the issues"
git push origin eco_edits
```

This will then trigger the original pull request to try again and the checks will run again. If the checks pass then the pull request can be merged. If the checks fail then the pull request will not be merged and the checks will need to be fixed.

If someone attempts to make these changes from a different branch of repository then those changes will be rejected by the checks on the pull request. Top level changes to the eco-model can only be made using pull requests from the configured repository for the Ecosystem.

## Making further changes to ecosystem

This is slightly different from the initial step as we were cloning the system from the datasurfacetemplate repository. At this point, we have our own repository in test-surface. So, when we make changes, we clone that, make the changes in the eco_edits branch and then push them to the main branch. We can then make a pull request to the main branch to get the changes live.

```shell
We will add an Azure Dataplatform and make it the default. We will also add a GovernanceZome to create an simple complete Ecosystem model.

cd ~/models/AcmeEcoMain
git clone https://www.github.com/billynewport/test-surface
```

This creates a test-surface folder containing the current live branch of the ecosystem model. We can now edit the eco.py file to add the AzureDataplatform and a GovernanceZone.

```python

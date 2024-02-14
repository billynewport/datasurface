# What is a Data ecosystem?

Every enterprise has a data ecosystem. It comprises all the data within an enterprise, how it moves throughout the enterprise, the technologies used to process, store and move that data. Governance is about analyzing the ecosystem and making sure it meets policies designed to ensure best practises, keep the data safe and enforce corporate or governmeny policies/laws on storing sensitive data.

Datasurface tries to simplify this ecosystem providing a platform which can describe this ecosystem and then automatically provision as much of the infrastructure to implement it as possible.

## Creating an empty Ecosystem

This can be done by setting up the main repository. The repository should have a requirements.txt which include the datasurface python package from pypi. The datasurface action handlers should be setup in a workflow. Once this is done then a starter eco.py file should be created in the root folder. Datasurface invokes the function createEcosystem() -> Ecosystem inside the eco.py module.

Here is an example starter:

```python
from datasurface.md.AmazonAWS import AmazonAWSDataPlatform
from datasurface.md.Azure import AzureDataplatform, AzureKeyVaultCredential
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import DefaultDataPlatform, Ecosystem, GovernanceZoneDeclaration, InfrastructureLocation, InfrastructureVendor

# Define a test Ecosystem
def createEcosystem() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitHubRepository("owner/surfacerepo", "main"),
        # Declare the Azure Data Platform and make it the default
        DefaultDataPlatform(AzureDataplatform("Azure Platform", AzureKeyVaultCredential("vault", "maincred"))),
                              
        GovernanceZoneDeclaration("Azure_USA", GitHubRepository("owner/azure_usa", "main")),
        GovernanceZoneDeclaration("Azure_EU", GitHubRepository("owner/azure_eu", "main")),
        GovernanceZoneDeclaration("Azure_China", GitHubRepository("owner/azure_china", "main")),

        InfrastructureVendor("Azure",
            PlainTextDocumentation("Microsoft Azure"),
            InfrastructureLocation("USA",
                InfrastructureLocation("Central",
                    InfrastructureLocation("Central US"), # Iowa
                    InfrastructureLocation("North Central US"), # Illinois
                    InfrastructureLocation("South Central US"), # Texas
                    InfrastructureLocation("West Central US")), # Wyoming
                InfrastructureLocation("East",
                    InfrastructureLocation("East US"), # Virginia
                    InfrastructureLocation("East US 2"), # Virginia
                    InfrastructureLocation("East US 3")), # Georgia
                InfrastructureLocation("West",
                    InfrastructureLocation("West US"), # California
                    InfrastructureLocation("West US 2"), # Washington
                    InfrastructureLocation("West US 3")) # Arizona
            ))
        )
    return e
```

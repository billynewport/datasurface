# Creating the initial ecosystem model

[DataSurface has a starter repository](http://www.github.com/billynewport/datasurfacetemplate) which can be cloned to get started. To get started, we need to make a new github repository which will contain the live version of your ecosystem model.

This is named <http://www.github.com/billynewport/test-surface> in this tutorial. Please use your repository names. The main branch will be the live model. We will create the test-surface repository in github. We now have an empty git repository. Next, we will clone the starter repository and then push it to this repository.

```shell
mkdir ~/models
mkdir ~/models/AcmeEcoMain
cd ~/models/AcmeEcoMain
git clone https://www.github.com/billynewport/datasurfacetemplate
```

This clones the repository in to the datasurfacetemplate folder in the current directory (~/models/AcmeEcoMain). Next, we need to edit the eco.py file to change the main repository for the model to this repository. The datasurfacetemplate folder is just for this initial seeding. Once, this is checked in to your repository then the name of your own repository will be used as you will be cloning from that repository after setup.

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

## General procedure to make changes

From now on, we will make changes to the ecosystem using the eco_edits branch. We will clone the current live model, make the changes in the eco_edits branch and then push them to the main branch. We can then make a pull request to the main branch to get the changes live. When the pull request is approved and merged then we can delete the eco_edits branch and the cycle is complete. We repeat these steps for each change to the ecosystem.

Likewise, for changes to different parts of the model which have their own repository or branch, we do the same thing. Clone to the edits branch of the specified repository for that part of the model. Make the changes, push the changes back to the edit branch and then make a pull request to the test-surface/main repository.

## Procedure to make changes using vscode

See [here for the procedure to make changes using VS Code](vscodeeditbranch.md).

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
cd ~/models/AcmeEcoMain
git clone https://www.github.com/billynewport/test-surface
cd test-surface
# Use your favorite editor to make the above changes to eco.py
git add .
git commit -m "Added Azure vendor and locations"
git checkout -b eco_edits
git push origin eco_edits
```

Next, head to github.com and make a pull request from the eco_edits branch to the main branch. The github checks will run that were added from the starter project. These check that there are no syntax errors in the eco.py file, that the pull request only contains .py files and that the changes are valid/self consistent and are compatible with the existing model. If these checks fail then you can perform the following steps to fix the issues so that the checks will succeed.

```shell
cd ~/models/AcmeEcoMain
git clone https://www.github.com/billynewport/test-surface
cd test-surface
# Edit the eco.py file and fix the issues
git add .
git commit -m "Fixed the issues"
git push origin eco_edits
```

This will then trigger the original pull request to try again and the checks will run again. If the checks pass then the pull request can be merged. If the checks fail then the pull request will not be merged and the checks will need to be fixed.

If someone attempts to make these changes from a different branch of repository then those changes will be rejected by the checks on the pull request. Top level changes to the eco-model can only be made using pull requests from the configured repository for the Ecosystem.

## Making further changes to ecosystem, lets define a GovernanceZone 'GZ'

This is slightly different from the initial step as we were cloning the system from the datasurfacetemplate repository. At this point, we have our own repository in test-surface. So, when we make changes, we clone that, make the changes in the eco_edits branch and then push them to the main branch. We can then make a pull request to the main branch to get the changes live.

```shell
We will add an Azure Dataplatform and make it the default. We will also add a GovernanceZome to create an simple complete Ecosystem model.

cd ~/models/AcmeEcoMain
rm -rf test-surface
git clone https://www.github.com/billynewport/test-surface
```

This creates a test-surface folder containing the current live branch of the ecosystem model. We can now edit the eco.py file to add the AzureDataplatform and a GovernanceZone.

```python
from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import CloudVendor, InfrastructureLocation, InfrastructureVendor, DefaultDataPlatform, GovernanceZoneDeclaration
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Azure import AzureBatchDataPlatform, AzureKeyVaultCredential


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
                    InfrastructureLocation("West US 3")))),  # Arizona
            DefaultDataPlatform(
                AzureBatchDataPlatform(
                    "AzureBatch",
                    PlainTextDocumentation("Azure Batch"),
                    AzureKeyVaultCredential("keyvault", "mysecret"))),
            GovernanceZoneDeclaration("GZ", GitHubRepository("billynewport/test-surface", "gz_edits"))
        )
```

This defines a default data platform and a governance zone. The default data platform is an AzureBatchDataPlatform with a key vault credential. The governance zone is named GZ and is owned by the test-surface repository. We can now check in these changes from the eco_edits branch to the main branch.

```shell
cd ~/models/AcmeEcoMain
# Make the changes to eco.py above
git add .
git commit -m "Added AzureBatchDataPlatform and GovernanceZone"
git checkout -b eco_edits
git push origin eco_edits
```

Now, we can do the pull request on github.com. The checks will run and if they pass then the pull request can be merged. If they fail then the pull request will not be merged and the checks will need to be fixed.

At this point, we have an Ecosystem "AcmeEco" defined. A single vendor with some locations for Azure. A single batch Azure DataPlatform added which is the default also. Lastly, a single GovernanceZone "GZ". This is where we will focus from now on. We will add a Team to the GovernanceZone and then create a data producer (data available using CDC in a managed SQL Server database) whose data we want to use and a single consumer that wants to use that data in a Lakehouse using a python notebook.

## Switching to gz_edits branch, declaring our Team

We just added a GovernanceZone which allows edits from the gz_edits branch. We cannot change the GovernanceZone using the eco_edits branch. We can try here to show that changes to the zone are rejected by the checks.

First, lets grab the model so we can edit it.

```shell
cd ~/models/AcmeEcoMain
rm -rf test-surface
git clone https://www.github.com/billynewport/test-surface
cd test-surface
# Edit the eco.py file as follows
```

```python
from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import CloudVendor, InfrastructureLocation, InfrastructureVendor, DefaultDataPlatform, GovernanceZoneDeclaration
from datasurface.md.Governance import GovernanceZone, TeamDeclaration
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Azure import AzureBatchDataPlatform, AzureKeyVaultCredential


def createEcosystem() -> Ecosystem:
    e: Ecosystem = Ecosystem(
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
                    InfrastructureLocation("West US 3")))),  # Arizona
            DefaultDataPlatform(
                AzureBatchDataPlatform(
                    "AzureBatch",
                    PlainTextDocumentation("Azure Batch"),
                    AzureKeyVaultCredential("keyvault", "mysecret"))),
            GovernanceZoneDeclaration("GZ", GitHubRepository("billynewport/test-surface", "gz_edits"))
        )
    gz: GovernanceZone = e.getZoneOrThrow("GZ")

    gz.add(
        TeamDeclaration("DemoTeam", GitHubRepository("billynewport/test-surface", "demoteam_edits"))
    )

    return e

```

Here, we change it a little. We stash the model in a variable e. We then get the GovernanceZone "GZ". At this point, it's now being defined by making this call. This call can only be made from the gz_edits branch. We will try to check in the model using the eco_edits branch and see what happens.

```shell
cd ~/models/AcmeEcoMain
git add .
git commit -m "Added DemoTeam to GZ"
git checkout -b eco_edits
git push origin eco_edits
```

The pull request will fail checks with the following error in the "Run Check Script" section:

```text
 Ecosystem(AcmeEco)
   AuthorizedObjectManager(zones)
     ERROR:Key GZ has been added by an unauthorized source
Total errors: 1, warnings: 0
```

Lets try again with the edits in the gz_edits branch. Lets just switch branches and push it up and make a pull request.

```shell
cd ~/models/AcmeEcoMain
cd test-surface
git checkout -b gz_edits
git push origin gz_edits
```

Now, we can do the pull request on github.com. The checks will run and if they pass then the pull request can be merged. If they fail then the pull request will not be merged and the checks will need to be fixed.

## Switching to demoteam_edits branch, defining our Team, data producer and data consumer

We need to make a decision here. We can keep adding to the eco.py file or we can split it up in to separate files. We will split it up in to separate files. We will create a new file for the Team, DataProducer and DataConsumer. We will then import these in to the eco.py file. This is a purely subjective decision. Some kind of split will make sense as projects get bigger.

We will make a demo_team.py file which is where we will define the Team, producers and consumers.

### Defining a Data Producer

A Data producer is someone or a team that has data stored in somekind of data container, like an SQL database, and wants to make it available to the broader enterprise for their consumption. We will define a DataProducer called "DemoProducer" which has a SQL Server database with some data in it. We will use the Azure SQL Server database for this.

Each source of data should have a named Datastore. Each table or collection of similar records should have a named dataset within that Datastore. In our example, we will create a Datastore 'USA_Customers' with 2 datasets, one for customers and one for orders. We will also add metadata which lets the platform know how to get the data that has been described. We will imagine the data is contained in an Azure SQL Server database which has CDC enabled and indicate that capturing every 10 minutes is acceptable.

We will create a file called demoteam.py and add the following code to it. This will create the store and datasets. We will then modify the eco.py file to call this to define the team.

These team level changes must come from the demoTeam repository and branch. This is the test-surface repository and the demoteam_edits branch. We will make the changes in the demoteam_edits branch and then make a pull request to the main branch.

```shell
cd ~/models/AcmeEcoMain
rm -rf test-surface
git clone https://www.github.com/billynewport/test-surface
cd test-surface
# Create a file called demoteam.py and add the following code to it
# Edit the eco,py file
git add .
git commit -m "Defined the DemoTeam and a simple producer"
git checkout -b demoteam_edits
git push origin demoteam_edits
```

Here is the demoteam.py file and the modified eco.py files for the above sequence.

```python
# demoteam.py
from datasurface.md import Ecosystem
from datasurface.md.Governance import GovernanceZone, Team, Datastore, CDCCaptureIngestion, \
    PyOdbcSourceInfo, CronTrigger, IngestionConsistencyType, Dataset
from datasurface.md.Schema import DDLTable, DDLColumn, VarChar, NullableStatus, PrimaryKeyStatus, SmallInt, Date, IEEE32
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Policy import SimpleDC, SimpleDCTypes
from datasurface.md.Azure import AzureKeyVaultCredential


# This defines a DemoTeam which contains a datastore producer with 2 datasets. These are the
# example datasets from the Northwind database. The data is ingested every 10 minutes using CDC

def defineDemoTeam(eco: Ecosystem) -> None:
    gz: GovernanceZone = eco.getZoneOrThrow("GZ")

    team: Team = gz.getTeamOrThrow("DemoTeam")

    team.add(
        PlainTextDocumentation("This is the demo team"),
        Datastore(
            "USA_Customers",
            PlainTextDocumentation("USA Customer data"),
            CDCCaptureIngestion(
                PyOdbcSourceInfo(
                    "US_NWDB",
                    eco.getLocationOrThrow("Azure", ["USA", "East", "East US"]),  # Where is the database
                    serverHost="tcp:nwdb.database.windows.net,1433",
                    databaseName="nwdb",
                    driver="{ODBC Driver 17 for SQL Server}",
                    connectionStringTemplate="mssql+pyodbc://{username}:{password}@{serverHost}/{databaseName}?driver={driver}"
                ),
                CronTrigger("NW_Data Every 10 mins", "*/10 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                AzureKeyVaultCredential("mykeyvault", "nwdb_creds")),
            Dataset(
                "customers",
                SimpleDC(SimpleDCTypes.PC3),
                PlainTextDocumentation("This data includes customer information from the Northwind database. It contains PII data."),
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("contact_name", VarChar(30)),
                    DDLColumn("contact_title", VarChar(30)),
                    DDLColumn("address", VarChar(60)),
                    DDLColumn("city", VarChar(15)),
                    DDLColumn("region", VarChar(15)),
                    DDLColumn("postal_code", VarChar(10)),
                    DDLColumn("country", VarChar(15)),
                    DDLColumn("phone", VarChar(24)),
                    DDLColumn("fax", VarChar(24))
                )
            ),
            Dataset(
                "orders",
                DDLTable(
                    DDLColumn("order_id", SmallInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("customer_id", VarChar(5)),
                    DDLColumn("employee_id", SmallInt()),
                    DDLColumn("order_date", Date()),
                    DDLColumn("required_date", Date()),
                    DDLColumn("shipped_date", Date()),
                    DDLColumn("ship_via", SmallInt()),
                    DDLColumn("freight", IEEE32()),
                    DDLColumn("ship_name", VarChar(40)),
                    DDLColumn("ship_address", VarChar(60)),
                    DDLColumn("ship_city", VarChar(15)),
                    DDLColumn("ship_region", VarChar(15)),
                    DDLColumn("ship_postal_code", VarChar(10)),
                    DDLColumn("ship_country", VarChar(15))
                    )
            )
        )
    )
```

We will now modify the eco.py file to call this to define the team like this:

```python
# eco.py
from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import CloudVendor, InfrastructureLocation, InfrastructureVendor, DefaultDataPlatform, GovernanceZoneDeclaration
from datasurface.md.Governance import GovernanceZone, TeamDeclaration
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Azure import AzureBatchDataPlatform, AzureKeyVaultCredential

from demoteam import defineDemoTeam


def createEcosystem() -> Ecosystem:
    e: Ecosystem = Ecosystem(
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
                    InfrastructureLocation("West US 3")))),  # Arizona
            DefaultDataPlatform(
                AzureBatchDataPlatform(
                    "AzureBatch",
                    PlainTextDocumentation("Azure Batch"),
                    AzureKeyVaultCredential("keyvault", "mysecret"))),
            GovernanceZoneDeclaration("GZ", GitHubRepository("billynewport/test-surface", "gz_edits"))
        )
    gz: GovernanceZone = e.getZoneOrThrow("GZ")

    gz.add(
        TeamDeclaration("DemoTeam", GitHubRepository("billynewport/test-surface", "demoteam_edits"))
    )

    # Define the DemoTeam in ths ecosystem
    defineDemoTeam(e)
    return e
```

Now, we goto github and create a pull request from demoteam_edits to the main branch like always. The handlers will execute and verify that the changes are valid, authorized, consistent and backwards compatible with the main model.

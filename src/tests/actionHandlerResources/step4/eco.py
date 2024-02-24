from datasurface.md.AmazonAWS import AmazonAWSDataPlatform
from datasurface.md.Azure import AzureDataplatform, AzureKeyVaultCredential
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import CloudVendor, DataPlatformPolicy, DefaultDataPlatform, Ecosystem, GovernanceZone, GovernanceZoneDeclaration, \
    InfraStructureLocationPolicy, InfraStructureVendorPolicy, InfrastructureLocation, InfrastructureVendor, TeamDeclaration
from tests.actionHandlerResources.step3.defineEU_GZ import defineEU_GZ
from tests.actionHandlerResources.step4.defineUSA_GZ import defineUSA_GZ

# Base branch for step 1, define an Ecosystem, data platforms, infrastructure vendors/locations and 3 Governance Zones


def createEcosystem() -> Ecosystem:
    e: Ecosystem = Ecosystem(
        "Test",
        GitHubRepository("billynewport/test_step1", "main"),
        DefaultDataPlatform(AzureDataplatform("Azure Platform", PlainTextDocumentation("Test"), AzureKeyVaultCredential("vault", "maincred"))),
        AmazonAWSDataPlatform("AWS Platform", PlainTextDocumentation("Test")),

        GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/test_step1", "USAmain")),
        GovernanceZoneDeclaration("EU", GitHubRepository("billynewport/test_step1", "EUmain")),
        GovernanceZoneDeclaration("UK", GitHubRepository("billynewport/test_step1", "UKmain")),

        # Infra Vendors and locations
        InfrastructureVendor(
            "AWS",
            CloudVendor.AWS,
            PlainTextDocumentation("Amazon AWS"),
            InfrastructureLocation(
                "USA",
                InfrastructureLocation("us-east-1"),  # Virginia
                InfrastructureLocation("us-west-1")),
            InfrastructureLocation(
                "UK",
                InfrastructureLocation("eu-west-1"),  # Ireland
                InfrastructureLocation("eu-west-2")),  # London
            InfrastructureLocation(
                "EU",
                InfrastructureLocation("eu-central-1"),  # Frankfurt
                InfrastructureLocation("eu-west-3"))),

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
                    InfrastructureLocation("West US 3")))))  # Arizona

    # Add the EU GZ and its policies limiting locations to EU only
    gzEU: GovernanceZone = e.getZoneOrThrow("EU")

    # Enumerate all EU locations for AWS
    allEULocations: set[InfrastructureLocation] = e.getLocationOrThrow("AWS", ["EU"]).getEveryChildLocation()

    gzEU.add(
        InfraStructureLocationPolicy("EU Only", PlainTextDocumentation("Test"), allEULocations),
        TeamDeclaration("ParisTeam", GitHubRepository("billynewport/test_step1", "ParisMain"))
    )

    # Define the EU team and datastores
    defineEU_GZ(gzEU, e)

    # Define the USA GZ, restrict to AWS East locations only, only the AWS Dataplatform is allowed and define the NY team
    gzUSA: GovernanceZone = e.getZoneOrThrow("USA")
    gzUSA.add(
        # AWS Locations only
        InfraStructureVendorPolicy("AWS Only", PlainTextDocumentation("Test"), {e.getVendorOrThrow("AWS")}),  # AWS Locations only
        # AWS USA locations
        InfraStructureLocationPolicy("AWS US Only", PlainTextDocumentation("Test"), e.getLocationOrThrow("AWS", ["USA"]).getEveryChildLocation()),
        # AWS Dataplatform only
        DataPlatformPolicy("AWS Platform only", PlainTextDocumentation("Test"), {e.getDataPlatformOrThrow("AWS Platform")}),  # AWS DataPlatform only
        # NY Team definition
        TeamDeclaration("NYTeam", GitHubRepository("billynewport/test_step1", "NYMain"))  # NY Team
    )

    defineUSA_GZ(gzUSA, e)

    return e

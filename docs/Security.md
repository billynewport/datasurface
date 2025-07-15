# Security

Permissioning users to be able to read data in a Workspace is an independent problem to a DataPlatform whose job is to ingest data from producers and make it available for consumers with Workspaces to use in what ever DataContainers they require.

Different DataSurface customers will have different security requirements. Datasurface will ship with several security implementations which customers can choose from or they can write their own.

Security systems will work with a DataPlatform to provide the necessary permissions to DataPlatform managed objects in a DataContainer. Thus, the security system will track for a given Workspace, which users are allowed to read that data in the Workspace. The security system can ask a DataPlatform for the list of consumer objects in a specific DataContainer for a Workspace. The security system can then create the ACLs needed to implement what the customer has asked for.

A GovernanceZone will specify the SecurityModule that should be used on all Workspaces owned by Teams managed by that zone. If a firm used multiple SecurityModules then a seperate zone should be used for each. This means Workspaces are managed by exactly one SecurityModule.

The SecurityModule DSLs will be managed by the GovernanceZone git repo. SecurityTeam can create the DSL artifact and then submit a pull request to the GovernanceZone to get it merged.

Teams can control how a Workspace is controlled by the SecurityModules for the Zone. There may be policies indicating that a Workspace containing any non public data must be controlled by the SecurityModule.

## An example to work through

A customer has an LDAP directory with a list of groups containing kerberos ids. There is a database table which contains a list of applications and which LDAP groups can be used for different roles for that application, including access to data.

The customer will provide a security mapping as a DSL in git. This security mapping will indicate the application database DataContainer and the LDAP connection info. The DSL will allow a Application object to be created in the DSL. This object will have a list of Workspaces owned by the application. Those Workspaces will be grouped and mapped with a set of roles which have read access to the data in those Workspaces.

The security system will then run multiple times a day to make sure the permissions in the database match those reflected by the DSL in the live Datasurface model and the views in the database.

TODO, once ZeroDataPlatform is working then we'll do security next.

## Security auditing

It may be important for security implementations to log when they run and what they did when they ran. Regulated firms may require permissions on read controlled objects to be updated within a maximum time period. For example, if someone leaves or changed department then those changes must be reflected on read permissions within some maximum time period. If these maximum times are exceeded then what is the impact? Do you turn off the DataContainer hosting Workspaces that may be out of date? Is there another documented procedure in this state to manage granting access to new people or removing old people. It all depends on the security policy of the firm.

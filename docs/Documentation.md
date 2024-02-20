# DataSurface model documentation

DataSurface allows various elements of its ecosystem model to be documented. DataSurface supports documentation in two formats, plain text or markdown. Elements that allow documentation typically take a Documentation object as a parameter to there constructor or add methods. The Documentation object can be a PlainTextDocumentation or a MarkdownDocumentation object. The MarkdownDocumentation object allows markdown to be used to format the documentation. The PlainTextDocumentation object allows plain text to be used to format the documentation.

The following elements support documentation objects:

* Ecosystem
* GovernanceZone
* Team
* Datastore
* Dataset
* DataPlatform
* Policy objects and their subclasses
* Workspace
* DataTransformer
* Repository and subclasses
* DDLColumn

# VSCode extension for DataSurface

This is an extension which can be configured to point at the main branch for a model in a git repository. There would be a key stroke or menu option which will refresh the main branch and another keystroke or menu option which will run the git action comparing the current model to that main branch, reporting the warnings and errors of the lint check.

## Configuration

The extension needs to track a supported repository (github or gitlab for now or a local folder). We may also need a credential or to be signed in to that repository to do a clone command.

## Refresh Main Branch

This keystroke will do a local clone of the selected main branch. For local folder, it lets you specify a different folder where the user would have done this operation.

## Compare Current Model to Main Branch

DataSurface includes action handlers to compare the model in a pull request against the current model in a branch. The extension is going to have to call the pythn code in datasurface to do this lint check and then using the returned list of ValidationProblems (warnings and errors), show them to the vscode developer and ideally mark lines of code in the developers workspace showing the problems attached to the line of code. DataSurface ValidationProblems already track the file and line number where a UserDSLObject was constructed.

This is basically identical to the git action workflow thats already coded which is given 2 folders and performs this task. For the authorization checks to work, it does need to be provided the with appropriate RepositorywithCICD instance.

## Python environment

The user's DataSurface model project will already have a working and compatible python environment configured. The extension can use this environment to run the datasurface code.

## Implementation

Asked Cursor just now to show me what such an extension would look like and it seems straightforward, not a lot of code. I'd imagine it's harder than it looks as it always is but it's very doable.

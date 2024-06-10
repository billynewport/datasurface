# Edit a branch in Visual Studio Code

Here are the steps to clone a branch, edit it, commit to a new branch, and then push to that branch using Visual Studio Code (VS Code):

## Clone

Clone the repository: Open the Command Palette with Cmd+Shift+P (Mac) or Ctrl+Shift+P (Windows/Linux), then type Git: Clone and press Enter. Paste the URL https://www.github.com/billynewport/test-surface and press Enter. Choose a directory on your local machine to clone the repository to.

## Switch to branch

Switch to the branch you want to edit: Open the Source Control view with Cmd+Shift+G (Mac) or Ctrl+Shift+G (Windows/Linux). At the top, you'll see the current checked-out branch. Click on it and select the branch you want to edit from the dropdown list.

## Edit files

Edit the files: Navigate to the files you want to edit in the Explorer view (Cmd+Shift+E on Mac or Ctrl+Shift+E on Windows/Linux) and make your changes.

Create a new branch: Go back to the Source Control view, click on the current branch at the top, then select Create new branch... from the dropdown list. Enter a name for your new branch and press Enter.

## Commit and push

Commit your changes: In the Source Control view, you'll see a list of changes. Enter a commit message in the text box at the top, then click the checkmark icon to commit the changes to your new branch.

Push the new branch: Open the Command Palette again and type Git: Push. Select Push to... and then select your new branch. This will push your new branch and the commit to the remote repository.

## Create a pull request

Create a pull request: This step is not done in VS Code, but on GitHub's website. Go to the page for your repository on GitHub, switch to your new branch using the Branch dropdown, then click New pull request. Select the original branch you edited as the base branch, review your changes, then click Create pull request.

## Conclusion

Once pull request passed checks and is approved then the changes will be merged into the original branch.

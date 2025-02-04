# Using DataSurface with other repositories

DataSurface is independent of GitHub. It has a provided GitHub plugin which allows it to integrate with the GitHub CI/CD mechanisms. Any CI/CD capable repository can be similarly used with DataSurface.

There is a Repository base class in the model. This has a subclass GitHubRepository. Another subclass can be easily added for a different repository. The code for check incoming pull requests for authorization, backwards compatibility, consistency and so on is independent of GitHub and works with Repository objects.

DataSurface comes with a github action handlers and workflow yaml files but these can be replaced with any other CI/CD system. Those files are just a convenience for GitHub users.

To summarize, there is nothing specific to GitHub in DataSurface. DataSurface comes with an implemented interface for GitHub but it's easy to add another interface for another repository. The model is independent of the repository. The model is the same whether it's used with GitHub or another repository.

## How to integrate with a different CI/CD system

### Create a subclass of the Repository class for your version control system

First, create a subclass for Repository which allows you to describe an "endpoint" in the other repository thats similar to repository/branch in GitHub. An instance of this class should be able to represent a single version of the model in that repository.

### Provide a subclass of RepositoryCICD and a workflow plugin for your CI/CD system

Next, file in src/datasurface/handler called pull-request.yml is what called the pull request validation in DataSurface. The workflow describe in this file works as follows and would need to be replaced with a similar workflow for the other repository:

* Check out the incoming pull request code to a folder 'pr'
* Check out the existing code to a folder called 'main'
* Setup python and install the datasurface package. This is done by using pip to install the python modules in the requirements.txt file.
* Run the python module datasurface.handler.action passing the parameters for the paths to the main and pr folders above.
* If this action.py exists with non zero then the pull request is rejected.

The datasurface.handler.action.py file is a template showing how to implement a GitHub CICD workflow plugin. It subclasses the RepositoryWithCICD class with a GitHub specific subclass. It currently uses some github specific environment variables such as GITHUB_TOKEN and the name of the repository/branch for the incoming pull request. These are used to create a Repository object describing that pull request source. This would be changed to create an instance of the gitlab or bitbucket repository subclass. All of the code for handling the pull requests is contained within the base class RepositoryCICD class. This class is independent of the repository and can be used with any repository.

### Provide a workflow to run the CICD specific module defined above

This module should be executed on every pull request equivalent. It should be run and invoke the DataSurface pull request checker. If the checker fails then this workflow should fail the pull request. The github version of this is contained in src/datasurface/handler/pull-request.yml

### Provide a workflow to restrict the files which can be changed by a pull-request

A model is just the eco.py and associated python modules. When users submit pull requests to get their changes incorporated to the model then the model needs to be defended against changes which will undermine the controls. The github workflow "check-files-changed.yml" provided in the DataSurface template repo looks for changes which will undermine controls and fails the pull request if these occur. Examples of such changes would be:

* Attempts to modify the workflow files in the .github folder
* Attempts to modify the requirements.txt file

The set of actions specified here will be expanded as more vulnerabilities are discovered and fixed. The simplest way appears to simply be only allow a pull request to modify python files, i.e. files ending in .py. This is the default behavior of the check-files-changed.yml file. This file would need to be replaced with a similar file for the other repository.

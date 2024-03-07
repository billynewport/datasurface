# Using DataSurface with other repositories

DataSurface is largely independent of GitHub. It's very loosely coupled to GitHub. There is a Repository base class in the model. This has a subclass GitHubRepository. Another subclass can be easily added for a different repository. The code for check incoming pull requests for authorization, backwards compatibility, consistency and so on is independent of GitHub and works with Repository objects.

DataSurface comes with a github action handlers and workflow yaml files but these can be replaced with any other CI/CD system. Those files are just a convenience for GitHub users.

To summarize, there is nothing specific to GitHub in DataSurface. DataSurface comes with an implemented interface for GitHub but it's easy to add another interface for another repository. The model is independent of the repository. The model is the same whether it's used with GitHub or another repository.

## How to integrate with a different CI/CD system

First, create a subclass for Repository which allows you to describe an "endpoint" in the other repository thats similar to repository/branch in GitHub. An instance of this class should be able to represent a single version of the model in that repository.

Next, file in src/datasurface/handler called pull-request.yml is what called the pull request validation in DataSurface. The workflow describe in this file works as follows and would need to be replaced with a similar workflow for the other repository:

* Check out the incoming pull request code to a folder 'pr'
* Check out the existing code to a folder called 'main'
* Setup python and install the datasurface package. This is done by using pip to install the python modules in the requirements.txt file.
* Run the python module datasurface.handler.action passing the parameters for the paths to the main and pr folders above. 
* If this action.py exists with non zero then the pull request is rejected.

The datasurface.handler.action.py file is a template showing how to implement a GitHub CICD workflow plugin. It subclasses the RepositoryWithCICD class with a GitHub specific subclass. It currently uses some github specific environment variables such as GITHUB_TOKEN and the name of the repository/branch for the incoming pull request. These are used to create a Repository object describing that pull request source. This would be changed to create an instance of the gitlab or bitbucket repository subclass. All of the code for handling the pull requests is contained within the base class RepositoryCICD class. This class is independent of the repository and can be used with any repository.

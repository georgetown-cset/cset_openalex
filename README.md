# code-validation-template

This repository contains configuration files that will set up Python and SQL linting, Python test automation via Github Actions, and other format checks (json, yml) in a repository. You can either use this repository as a template to start new projects, or copy files into an existing repository.

To set up in an existing repository:

- `cp -r <path to this repo>/{.flake8,.pre-commit-config.yaml,.sqlfluff,pyproject.toml,.github,tests} .`
- Edit the tests dir or disable the test automation in `.github/workflows/main.yml` for now (but it's better if you go ahead and add some tests :) )
- `git add .flake8 .pre-commit-config.yaml .sqlfluff pyproject.toml .github/workflows/main.yml tests/<your tests>`
- Edit the `requirements.txt` to add more requirements if you want.
- `pip install -r requirements.txt`
- `pre-commit install`
- `pre-commit run --all-files`

You may get linting failures on your first run. You can re-run `pre-commit run --all-files` until you only see issues that need to be manually fixed.

See https://docs.sqlfluff.com/en/stable/gettingstarted.html for more information on how to run and configure sqlfluff, a SQL linter that is run as part of the pre-commit hooks.

Once you have the pre-commit hooks installed, they will run every time you `git commit`, preventing you from committing files that do not comply with the linters. These checks will also run as part of the Github Actions for your repository.

Please note, when adding this configuration to an existing repository it is preferred for you to make a commit limited to reformatting your codebase without making any other substantitve changes - making changes to your code while also reformatting it can make tracking down the substantive changes pretty difficult.

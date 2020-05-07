# Data Science Insights
This is one of the few data-science team repositories that will host many notebooks/projects, and will serve
as a place to put miscellaneous or one-off insights, single page learnings, literature reviews, how stuff works, etc.

#### Repository Status: Work in Progress (WIP)
TODO: Change the status to active after the readme is fully filled in.

## List of Active Projects
[How Loop Dosing Decision Making Works](how_loop_dosing_decision_works)

## Contributing Guide
1. All are welcome to contribute to this repository. See [Getting Started Below](## Getting Started with the Conda Virtual Environment)
2. Please keep in mind that this repository is for small one-off or miscellaneous projects 
where most of the code is in one file.
3. If you are going to contribute a notebook, put the notebook in the [notebooks folder](notebooks).
4. If you are going to contribute code, start with the [template](template), and rename it according
to the notebook naming rules. 

### Notebook and Project Naming Convention
1. The naming convention for notebooks and projects is as follows
`[short-description]--[initials]--[date-created]--[version]`,
e.g. `initial-data-exploration--jqp--2020-04-25--v-0-1-0.ipynb`.
A short `-` delimited description, the creator's initials, date of creation, and a version number 
separated by `--`.
1. Naming convention for data files, figures, and tables is
`[PHI (if applicable)]--[short_description]--[date created or downloaded]--[code_version]`,
e.g. `raw-project-data-from-mnist--2020-04-25--v-0-1-0.csv`,
or `project-data-figure--2020-04-25--v-0-1-0.png`.

NOTE: PHI data is never stored in github and the .gitignore file includes this requirement as well.

### Technologies Used Across All Projects
* Python 
* [Anaconda](https://www.anaconda.com/) for our virtual environments
* Pandas for working with data (99% of the time)
* Google Colab for sharing examples
* Plotly for visualization
* Pytest for testing
* Travis for continuous integration testing
* Black for code style
* Flake8 for linting
* [Sphinx](https://www.sphinx-doc.org/en/master/) for documentation
* Numpy docstring format
* pre-commit for githooks

## Getting Started with the Conda Virtual Environment
1. Install [Miniconda](https://conda.io/miniconda.html). CAUTION for python virtual env users: Anaconda will automatically update your .bash_profile
so that conda is launched automatically when you open a terminal. You can deactivate with the command `conda deactivate`
or you can edit your bash_profile.
2. If you are new to [Anaconda](https://docs.anaconda.com/anaconda/user-guide/getting-started/)
check out their getting started docs.
3. If you want the pre-commit githooks to install automatically, then following these
[directions](https://pre-commit.com/#automatically-enabling-pre-commit-on-repositories).
4. Clone this repo (for help see this [tutorial](https://help.github.com/articles/cloning-a-repository/)).
5. In a terminal, navigate to the directory where you cloned this repo.
6. Run `conda update -n base -c defaults conda` to update to the latest version of conda
7. Run `conda env create -f conda-environment.yml --name [input-your-env-name-here]`. This will download all of the package dependencies
and install them in a conda (python) virtual environment. (Insert your conda env name in the brackets. Do not include the brackets)
8. Run `conda env list` to get a list of conda environments and select the environment
that was created from the environmental.yml file (hint: environment name is at the top of the file)
9. Run `conda activate <conda-env-name>` or `source activate <conda-env-name>` to start the environment.
10. If you did not setup your global git-template to automatically install the pre-commit githooks, then
run `pre-commit install` to enable the githooks.
11. Run `deactivate` to stop the environment.

## Maintaining Compatability with venv and virtualenv
This may seem counterintuitive, but when you are loading new packages into your conda virtual environment,
load them in using `pip`, and export your environment using `pip-chill > requirements.txt`.
We take this approach to make our code compatible with people that prefer to use venv or virtualenv.
This may also make it easier to convert existing packages into pypi packages. We only install packages directly
in conda using the conda-environment.yml file when packages are not available via pip (e.g., R and plotly-orca).

## Tidepool Data Science Team
|Name (with github link)    |  [Tidepool Slack](https://tidepoolorg.slack.com/)   |
|---------|-----------------|
|[Ed Nykaza](https://github.com/[ed-nykaza])| @ed        |
|[Jason Meno](https://github.com/[jameno]) |  @jason    |
|[Cameron Summers](https://github.com/[scaubrey]) |  @Cameron Summers    |

## Known TODO items
- [ ] automate the process of finding all of the the TODO: comments in the code and put link here.

## Initial Setup Checklist
- [ ] Update repo settings in github (manual process)
    * [x] Update **Settings/Options/Repository name**
        * Name follows the `<team (optional)> - <type(optional)> - <one-to-three-word-description> - <initials (optional)>` in `lowercase-dash-format`.
    Examples:
        * `icgm-sensitivity-analysis` is used by all of Tidepool so no team is needed and is considered production code so no type is needed.
        * `data-scence-donor-data-pipeline` is only used by Data Science
        * `data-science-template-repository` is a template (type) used by Data Science Team
        * `data-science-explore-<short-description>` type of work is exploratory
        * `data-science-explore-<short-description>-etn` exploratory solo work has initials at the end
    * [x] Update **Settings/Options/Manage access**
        - [x] Invite data-science-admins team and give admin access
        - [x] Invite Data Science team and give write access
    * [ ] Update **Settings/Options/Manage access/Branch protection rules**
        - [x] Set _Branch name pattern_ to `master`
        - [x] Check _Require pull request reviews before merging_
        - [x] Set _Required approving reivews:_ to 1 for non-production code and 2 for production code
        - [x] Check _Dismiss stale pull request approvals when new commits are pushed_
        - [ ] TODO: add in travis ci instructions via _Require status checks to pass before merging_
- [ ] Fill in this readme. Everything in [  ]'s should be changed and/or filled in.
- [ ] After completing this checklist, move the completed checklist to the bottom of the readme
- [ ] Delete everything above the [Project Name]

# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/dscc-admin-ch/csvw-eo/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                          |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|---------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| src/csvw\_safe/\_\_init\_\_.py                |       11 |        0 |        0 |        0 |    100% |           |
| src/csvw\_safe/assert\_same\_structure.py     |       25 |        0 |       10 |        0 |    100% |           |
| src/csvw\_safe/constants.py                   |       49 |        0 |        0 |        0 |    100% |           |
| src/csvw\_safe/csvw\_to\_opendp\_context.py   |       31 |        1 |        6 |        1 |     95% |       169 |
| src/csvw\_safe/csvw\_to\_opendp\_margins.py   |       32 |        0 |       20 |        0 |    100% |           |
| src/csvw\_safe/csvw\_to\_smartnoise\_sql.py   |       39 |        2 |       20 |        2 |     93% |  152, 164 |
| src/csvw\_safe/datatypes.py                   |      133 |        0 |       56 |        0 |    100% |           |
| src/csvw\_safe/generate\_series.py            |      133 |        8 |       54 |        3 |     92% |295-298, 366-367, 372-376 |
| src/csvw\_safe/make\_dummy\_from\_metadata.py |       93 |        0 |       30 |        0 |    100% |           |
| src/csvw\_safe/make\_metadata\_from\_data.py  |      161 |        2 |       58 |        2 |     98% |  151, 448 |
| src/csvw\_safe/metadata\_structure.py         |      191 |        0 |       64 |        0 |    100% |           |
| src/csvw\_safe/utils.py                       |       50 |        0 |       22 |        0 |    100% |           |
| src/csvw\_safe/validate\_metadata.py          |        7 |        0 |        0 |        0 |    100% |           |
| src/csvw\_safe/validate\_metadata\_shacl.py   |       12 |        0 |        0 |        0 |    100% |           |
| **TOTAL**                                     |  **967** |   **13** |  **340** |    **8** | **98%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/dscc-admin-ch/csvw-eo/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/dscc-admin-ch/csvw-eo/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/dscc-admin-ch/csvw-eo/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/dscc-admin-ch/csvw-eo/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fdscc-admin-ch%2Fcsvw-eo%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/dscc-admin-ch/csvw-eo/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.
# Projects

The table below lists completed and ongoing Price Points projects. Each row
contains a link to the project subdirectory and post/publication (if it has
been published). Each project subdirectory contains a README with the
project's data, methods, and references.

| Date    | Project Name                                              | Project Link                                         | Post Link                                                      |
|:-------:|-----------------------------------------------------------|------------------------------------------------------|----------------------------------------------------------------|
| 2025-04 | How much does it cost to give birth in the United States? | [./2025_04_delivery_costs](./2025_04_delivery_costs) | [/delivery-costs](https://pricepoints.health/p/delivery-costs) |

## Using the project template

[0000_00_proj_template/](./0000_00_proj_template/) contains a Python
project template that should be copied for each Price Points project. To
create a new project:

- Clone `pricepoints` and create a new git branch
- Copy the template subdirectory to a new subdirectory that reflects the
  project name
- Update the content of the new project's `pyproject.toml` and `README.md` files
- Remove any example files
- Add dependencies in `pyproject.toml` and use `uv lock` to update the lockfile
- Update this file with a link to the new subdirectory

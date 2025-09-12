# Projects

The table below lists completed and ongoing Price Points projects. Each row
contains a link to the project subdirectory and post/publication (if it has
been published). Each project subdirectory contains a README with the
project's data, methods, and references.

| Date    | Project Name                                                                       | Project Link                                         | Post Link                                                      |
|:-------:|------------------------------------------------------------------------------------|------------------------------------------------------|----------------------------------------------------------------|
| 2025-04 | How much does it cost to give birth in the United States?                          | [./2025_04_delivery_costs](./2025_04_delivery_costs) | [/delivery-costs](https://pricepoints.health/p/delivery-costs) |
| 2025-05 | Orange County’s big healthcare brawl                                               | [./2025_05_bsca_hoag](./2025_05_bsca_hoag)           | [/bsca-hoag](https://www.pricepoints.health/p/bsca-hoag)       |
| 2025-05 | The 340B program has gone off the rails                                            | [./2025_06_il_340b](./2025_06_il_340b)               | [/il-340b](https://www.pricepoints.health/p/il-340b)           |
| 2025-06 | The OBBB may disqualify hundreds of hospitals from the 340B program                | [./2025_06_il_340b](./2025_06_il_340b)               | [/obbb-340b](https://www.pricepoints.health/p/obbb-340b)       |
| 2025-07 | Secretive vendors are exploiting a free money glitch in the U.S. healthcare system | [./2025_07_blues](./2025_07_blues)                   | [/blues](https://www.pricepoints.health/p/blues)               |

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

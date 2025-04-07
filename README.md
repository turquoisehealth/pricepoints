# Price Points

**Price Points** is a [publication](https://pricepoints.health) that creates
novel, quantitative, public research using [Turquoise Health price data](https://turquoise.health/products/clear_rates_data)
and other healthcare datasets. This repository contains replication code
for all [Price Points](https://pricepoints.health) projects, as well as general
research and exploratory analyses related to healthcare prices in the
United States.

We're making this repository open-source because we want to:

- Publish our methods for scrutiny and replication by industry experts,
  academics, etc.
- Build a community of practice around price transparency data research
- Provide examples of working with/analyzing Turquoise Health data
- Support developers and others who want to perform their own analyses

For a full list of completed and ongoing research projects, see the
[Projects README](./projects/README.md).

> [!IMPORTANT]
> All projects are released on an informational basis only and are not official
> Turquoise Health products. Please use the [dedicated email](#contact)
> below for any questions about Price Points projects.

## FAQs

#### What input data is used?

Price Points primarily uses Turquoise Health [hospital and payer rates
data](https://turquoise.health/products/clear_rates_data) for its research.

Secondary data includes [Census/ACS data](https://www.census.gov/data/developers/data-sets/acs-5year.html),
[TIGER/Line shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html),
[travel times](https://opentimes.org), [Dartmouth Atlas data](https://data.dartmouthatlas.org/), etc.

#### How do I get access to replication data?

Whenever possible, Price Points will publish replication data along with each
analysis. Such data is typically a subset or aggregated version of the
underlying Turquoise Health rates data.

In cases where publishing replication data isn't possible (due to size
or licensing restrictions), we'll try to make the data available via other
means e.g. research/licensing agreements, the Turquoise Community tier, etc.

#### I want to do my own research. How do I get access to the underlying data?

If you're a researcher and want to use Turquoise Health data, you currently
have three options. In order, from least to most access:

1. [Request access](https://turquoise.health/researchers) to Turquoise research
  datasets, which contain a limited subset of hospital and payer negotiated
  rate data.
2. Request (free) researcher access to the Turquoise backend through the
  Community Tier. This provides nearly full access to the main Turquoise
  Health rates tables, but limited customer support.
3. [Contact](https://turquoise.health/products/clear_rates_data) the Turquoise
  Health sales team for full access to the underlying data (including
  historical rates).

#### What's in-scope for Price Points research projects?

Any research related to healthcare is in-scope, as long as the data exists
to support the analysis. That said, we tend to choose projects that most
benefit from the scale of Turquoise rates and other national datasets. That
means analyzing things at the national or state level, rather than at
the individual payer/provider level.

#### I think your analysis is wrong, what should I do?

We try to show our work, fact-check/review rigorously, and incorporate solid
domain knowledge, but healthcare is complicated and we won't always get
things right. If you've found an error, bad assumptions, or missing context in
one of our research projects, please create a
[GitHub issue](https://github.com/turquoisehealth/pricepoints/issues)
or [reach out](#contact) directly.

#### I have specific questions about an analysis, who can I contact?

See the [Contact](#contact) section below.

#### Who's behind this project?

I’m [Dan Snow](https://github.com/dfsnow), a data scientist and policy wonk
currently living in the Bay Area. I'm employed by
[Turquoise Health](https://turquoise.health/), which provides the data,
domain knowledge, and funding needed to do this work.

## Structure

This repository contains three sections:

1. [Projects](./projects/) - Contains the code and data for each project. See
  the [dedicated README](./projects/README.md) for more info.
2. [Analyses](./analyses/) - Contains exploratory and one-off research code
  unrelated to any specific project.
3. [Packages](./tq/) - Dedicated Python package for this repository.
  Contains helper functions, DB connections, etc.

## License

Code in this repository uses the [MIT](https://www.tldrlegal.com/license/mit-license)
license. Datasets and other linked assets may use different licenses.

## Attribution

Please cite Price Points work where appropriate. See the
[CITATION file](./CITATION.cff) or the GitHub sidebar for APA/BibTeX citation
templates.

## Contact

You can reach out to Dan directly at
[dan.snow@turquoise.health](mailto:dan.snow@turquoise.health).

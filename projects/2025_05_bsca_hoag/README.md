# Orange County’s Big Healthcare Brawl

## Overview

This project uses [price transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency)
and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC)
data to

It finds clear price patterns based on geography and type of care.
It suggests that national or regional analyses using price transparency data
are possible but difficult, and highlights some relevant data and
methodological challenges.



Side note: Neither BS of CA or Hoag are customers
What we want to do is look at the orange country payer / provider market and contextualize these on-going negotiations to help inform the conversation. Some obvious questions to explore:

- Are Hoag’s payments from BC of CA in line with the market?
- How does Hoag compare to the local market?
- How does BC of CA compare to the local market?
- Any other interesting trends or contextualization of the local market?

## Notes

- Includes CMS shoppable services
- Triple check with the data team that the rates we pull,w are correct
- Check for carveouts
- https://www.reddit.com/r/orangecounty/comments/1d8yw3r/blueshield_providence_hmo_cancelled_aug_1/
- UC currently also in talks with BSCA

## Outline

- Hoag gets higher rates from BSCA than competitors
- It also gets the same high rates in Irvine, where its competitors make far less
- Hoags highest utilization codes are X, with BSCA they're Y
- BSCA is X% of Hoag's claims
- BSCA reimbursement is super important to Hoag
- Hoag still uses APC billing, other providers use surgery groupers. Probably BSC wants them to switch
- What does Hoag get paid by other payers for the same codes?

- Blue Shield cares about absolute price and rate of change
- CalPERS is its own separate rate

## Plots

- Hoag v BSCA YoY trends
  - Is there something specific about Hoag

- Hoag highest utilization codes
- For codes that have prices across providers, show comp graph
- Show Hoag's reimbursement across payers
- Hoag prices at Irvine
- Breakdown of billing code type
- Hoag vs other Hoag-like providers in Cali (John Muir, Cedar Sinai, etc)

## Todo

- Dig into specific comparable hospitals to Hoag in Orange county
- Hospitals only make money from scheduled outpatient procedures
  Expect to see the most variation in such rates
- Look for a trend over time in BSC v Hoag rates. Try RC 272 (NICU per diem)
- Look for unsustainable trend in rates
- "I think you need ready access to hospital charge relativities (sometimes the out of the box index is the Cost to Charge Ratio CCR in the medicare cost report). That way you can keep an eye on when % of charge rates combined with list prices are out of whack. Chansoo and Stefano know more about this"
- Look at their cost structure via CRR
- Look at BSC market share in the area. Is there a reason this relationship is so special?
- Directly compare BSC to Anthem within Hoag

## From Ryan

On the second question, here's how I'd think about it. Defining a "cohort" is super challenging, but here are the factors I think matter most:
Geographic overlap - who operates in the same catchment or referral area? This can be fairly straight forward- who else is in a CBSA, market?
Service line overlap - where do we compete on services? Which services are provided? What is the overlap of high margin / high volume sercvices?
Structural leverage (and maybe brand?) - this is more about who is in the market. Examples: An independent hospital likely has less leverage than a system. A system that has a owned payer arm may also be in a more complex place than a system without. Brand names like Mayo command more leverage just by virtue of their care/quality/reputation.
Also interesting to think about overlap of payer networks. Any hospitals you're comparing should have roughly the same payer mix. It's hard, as an example, to compare a closed panel model like Kaiser to others.
Historical competitiveness + rate structures (hard to assess) - this is tough because that's part of what we're trying to solve. A system like Mayo that has straight % of charge contracts is hard to compare to another large system that has more standard modern contracts. It's not that you can't compare them, it's just that the comparison comes with an *.
Hard & fast stuff
Hospital type is important - you can't perfectly compare a 99 bed hospital to 1,000 bed AMC. Same with critical access hospitals / etc.
Net patient revenues is generally a good indicator of comparability on size / scope
Using CMI is sorta interesting - ie gauging how complex the population is. This can be a proxy for the level of advanced care a hospital provides.
Crow flight distance or drive time distance - where do patients actually go within a radius?
So really, it's a mix of many things.
On the first one, i'll start by giving my thoughts and then I can dive in after & do more research. On the provider side, they are generally trying to maximize net patient revenue. There are obviously operational ways to do this (lowering internal cost, shifting site of service, etc) but w/r/t negotiations & payer relation motions, it will come down to levers. Payers are trying to obviously minimize their spend, and typically have more leverage to do so. Payers find ways to squeeze providers all the time - they can try to lower rates across the board, they can try to reformat contracts into more favorable & predictable (for them) case rates, they can put pressure on administrative burdens (like authorization requirements), or could even carve up networks in such a narrow fashion that you as a provider are forced into a major discount from the standard PPO rate in order to just keep patients in the door. There are some payers that are even more explicit with how they control the relationship- going as far as "most favored nation clauses" (explicitly or implicitly) where a payer basically demands that they have the best (lowest) rates in the entire market with a provider. They will seek sub-parity if they find that they aren't (which is complicated by the existence of transparency data).
Providers have certain levers to pull. In areas where they have percent of charge contracts (more favorable in that they have control over charges) they may seek strategic pricing initiatives to better control their revenues. Payers have limitations, though, in that you may only be able to increase charges x percent. This can create challenges with how you price system wide, as you can't charge payer A patients more than payer B. Providers also, from a brand perspective may just have the upper hand and know that the payer needs them in order to offer a strong & compelling network. They would seek to be the highest paid in the market. Transparency data can be used here.
One problem, as you know, is that because A) PT data isn't actually perfectly compatible for claims pricing and B) contracts differ, it's hard to really understand who is getting the best rates in a market. One theme i've observed is payers/providers using PT data to cherry pick without context. They may say: "the hospital down the road is getting a DRG base rate of $15,000, but ours is $13,000". However, they have implants / drugs carved out & reimbursed at 60% of charge, whereas the other hospital doesn't have that.

# Correlation Detection

Correlation is a popular statistical measure that indicates whether and how two variables evolve in relation to each other. Correlation detection is a function that returns a correlation value between the two series, which can be positive, negative or neutral. The detection is based on observing whether there is any causal association between the two variables e.g., when rainfall increases, the production increases and the price decreases as a consequence. 

## Example
Let's assume we are interested in calculating the correlation between railfall and corn prices.

## Configuring the correlation
The correlation is fully configurable by the user, read in a JSON format. Every correlation configuration consists of three properties as shown below:
- *Source:* An RDF data source where a correlation predicate is found (lines 4 and 12)
- *Target:* The actual RDF predicate to relate to e.g., rainfall (line 5), corn price (line 13).
- *Filters:* Set the value range where the correlation needs to be computed. For example, the corn price RDF data contains prices for several years and months, but we might be only interested in year 2015 (line 2015) and months 1 to 4 (line 8). Noticing the latter, the filter can be on multiple values separated by comma. It is also possible to obtain the values on aggregating multiple other values. For example, as we are correlating the rainfall aggregate values with the corn prices, it only makes sense if we consider the corn prices of later months than the month where the rainfall values are recorded. In that case, we should provide how those values can be aggregated (line 15), e.g. summing them up, or computing their average value, etc. 

Applied to our example, every rainfall month (line 8) is correlated to the sum of the prices of subsequent months (line 15) of the same year 2015 (lines 7 and 16):
- Prices of month 1 with the average of months 02, 03 and 04.
- Prices of month 2 with the average of months 03, 04 and 05.
- Prices of month 3 with the average of months 04, 05 and 06.
- Prices of month 4 with the average of months 05, 06 and 07.

```scala
1. {
2. "properties": [
3.   {
4.     "source": "/path/to/RainfallCSVToRDF/output.ttl",
5.     "target": "<http://purl.oclc.org/NET/ssnx/cf/cf-feature#rainfall_amount>",
6.     "filters": {
7.       "<http://dbpedia.org/ontology/year>": "2015",
8.       "<http://dbpedia.org/ontology/month>": "1,2,3,4"
9.     }
10.   },
11.   {
12.     "source": "/path/to/PricesCSVToRDF/output.ttl",
13.     "target": "<http://comicmeta.org/cbo/price>",
14.     "filters": {
15.       "<http://dbpedia.org/ontology/month>": "{02,03,04}{03,04,05}{04,05,06}{05,06,07};sum",
16.       "<http://dbpedia.org/ontology/year>": "2015"
17.     }
18.   }
19. ]
20. }
```

##### SANSA Correlation Detection was built during and for the [BETTER project](https://www.ec-better.eu/pages/better-project), a European Union’s Horizon 2020 Research and Innovation Programme under grant agreement no 776280.

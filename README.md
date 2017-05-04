# Hadoop Text Analyzer

The analyzer has to build a table that shows the occurrences of the words that appear together in the given text. Each line is split into context words and query words in the mapper phase. The reducer combines the contextWord and list of queryWords into a table:


contextword1
queryword1, occurrence
queryword2, occurrence


contextword2
queryword1, occurrence
queryword2, occurrence

## Example 

Text: “Mr. Bingley was good-looking and gentlemanlike; he had a pleasant countenance, and easy, unaffected manners.”


For the word Bingley, “and” appears twice and “gentlemanlike” appears once. In this calculation, we say Bingley is a ‘contextword’; “and” and “gentlemanlike” are ‘query- words’. The other example: for the contextword and, “pleasant” appears once, “and” appears once (not zero times or twice).

NOTE:
* Any character other than a-z, A-Z, or 0-9 should be replaced by a single-space character
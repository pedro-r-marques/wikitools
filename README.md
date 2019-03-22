# wikitools

Tools to process wikipedia data for ML purposes.

## pageviews

Processes montly wikipedia page statistics for a month and generates a pagecount file sorted in descending order.
It is designed to obtain the top N (e.g. top 1M) most popular wikipedia pages.

## sqlfilter

The raw wikipedia sqldumps are too slow to import in my dev system. This tool allows one to filter the sqldumps so that only entries for the top N pages are added to the database.

## wiki_entity_vec

Tools to generate a sparse matrix and train an embedding vector model.

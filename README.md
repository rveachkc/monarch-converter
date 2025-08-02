# monarch-converter

I wrote a script to convert transactions downloaded from Mint so they could be uploaded into Monarch Money

## Installation

```bash
pip install monarch-converter
```

## Usage

This script assumes you have a csv file downloaded from Mint.com before they shut down.

### Reading basic information

```bash
monarch-converter mint-transactions.csv
```

### Translating account names

There may be situations where an account name in Mint does not match what you want to use in Monarch.  These can be renamed in bulk with a csv file.

A helper has been created to format that csv file.

```bash
# create a helper csv - this will create a csv with duplicate account names
monarch-converter mint-transactions.csv --account-mapping-helper rename-accounts.csv
```

Just edit the `Monarch` column to use whatever account name you'd like, and then use it for the next step.
```bash
# this will read in the rename-accounts csv file, renaming account names as defined
monarch-converter mint-transactions.csv --account-mapping-translate rename-accounts.csv
```

### Output

Monarch Money requested a separate csv file for each account, so the data can be partitioned and chunked up.
By default, the max row count is 5000, but that is adjustable.

The output is specified by directory.

```bash
monarch-converter mint-transactions.csv --account-mapping-translate rename-accounts.csv --output ./output/ --max-rows 5000
```

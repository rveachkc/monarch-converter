import argparse
import logging
import os
from typing import Optional

import polars as pl
from slugify import slugify

MAX_ROW_DEFAULT = 5000


def split_dataframe_iter(df: pl.DataFrame, max_rows: int):
    """Splits a Polars DataFrame into smaller DataFrames of a specified maximum number of rows.

    Args:
        df (pl.DataFrame): The DataFrame to split.
        max_rows (int): The maximum number of rows for each split DataFrame.

    Yields:
        pl.DataFrame: A smaller DataFrame containing up to `max_rows` rows.
    """
    if max_rows <= 0:
        raise ValueError("max_rows must be greater than 0")

    for i in range(0, df.height, max_rows):
        yield df.slice(i, max_rows)


def main(
    input_file: str,
    output_dir: Optional[str] = "",
    account_mapping_helper_file: Optional[str] = "",
    account_mapping_translate_file: Optional[str] = "",
    max_rows: int = MAX_ROW_DEFAULT,
) -> None:
    logger = logging.getLogger(__name__)

    logger.info("Reading input file: %s", input_file)
    df = pl.read_csv(input_file)

    # convert date from MM/DD/YYYY to YYYY-MM-DD
    logger.info("Converting date format from MM/DD/YYYY to YYYY-MM-DD")
    df = df.with_columns(
        pl.col("Date").str.strptime(pl.Date, "%m/%d/%Y", strict=False).alias("Date")
    )

    # multiply amount by -1 if transaction type is debit
    logger.info("Adjusting Amount based on Transaction Type")
    df = df.with_columns(
        pl.when(pl.col("Transaction Type") == "debit")
        .then(pl.col("Amount") * -1)
        .otherwise(pl.col("Amount"))
        .alias("Amount")
    )

    if account_mapping_translate_file:
        map_translations = {
            row["Mint"]: row["Monarch"]
            for row in pl.read_csv(account_mapping_translate_file).iter_rows(named=True)
        }
        for mint_name, monarch_name in map_translations.items():
            logger.debug("Mapping '%s' to '%s'", mint_name, monarch_name)
        logger.info("Translating account names from %s", account_mapping_translate_file)
        df = df.with_columns(pl.col("Account Name").replace(map_translations))

    # drop, rename, and select columns to match output spec
    logger.info("Dropping and renaming columns to match Monarch output spec")
    df = (
        df.drop("Transaction Type")
        .rename(
            {
                "Description": "Merchant",
                "Original Description": "Original Statement",
                "Account Name": "Account",
                "Labels": "Tags",
            }
        )
        .select(
            [
                "Date",
                "Merchant",
                "Category",
                "Account",
                "Original Statement",
                "Notes",
                "Amount",
                "Tags",
            ]
        )
    )

    logger.info("Monarch Data Preview:%s%s", os.linesep, df.head(10))

    # only runs if an account mapping helper file is specified
    # this will create a stub file to help with renaming of the accounts
    if account_mapping_helper_file:
        mapping = pl.DataFrame(
            map(
                lambda x: {"Mint": x, "Monarch": x},
                df["Account"].unique().sort(),
            )
        )

        logger.info(
            "Writing account mapping helper file to %s", account_mapping_helper_file
        )
        print(mapping)
        mapping.write_csv(account_mapping_helper_file)

    # keep track of output filenames to avoid duplicates
    # this may mainly happen if two accounts have the same slugified name
    output_filenames = set([])

    def get_output_filename(account: str, fname_cnt: int) -> str:
        return f"monarch-{slugify(account, lowercase=True, separator='-', max_length=50)}-{fname_cnt}.csv"

    if output_dir:
        # partitions the dataframe by account
        for account, account_df in df.partition_by("Account", as_dict=True).items():
            account_name = account[0]
            fname_cnt = 1

            # this loop splits into smaller DataFrames if the account DataFrame exceeds max_rows
            for i_account_df in split_dataframe_iter(account_df, max_rows):
                # create a unique output filename for each df
                out_filename = get_output_filename(account_name, fname_cnt)
                while out_filename in output_filenames:
                    fname_cnt += 1
                    out_filename = get_output_filename(account_name, fname_cnt)
                # then lock it in
                output_filenames.add(out_filename)

                # write the DataFrame to a CSV file
                i_account_df.write_csv(os.path.join(output_dir, out_filename))
                logger.info(
                    "Writing %s rows of %s data to %s",
                    i_account_df.height,
                    account_name,
                    out_filename,
                )
                logger.debug(
                    "Data preview for %s:%s%s",
                    out_filename,
                    os.linesep,
                    i_account_df.head(5),
                )


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Monarch Converter CLI")
    parser.add_argument("input_file", type=str, help="Path to the input CSV file")
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        type=str,
        default="",
        help="Path to the output file (default: no output)",
    )
    parser.add_argument(
        "--account-mapping-helper",
        dest="account_mapping_helper_file",
        type=str,
        default="",
        help="Write a stub of an account mapping file",
    )
    parser.add_argument(
        "--account-mapping-translate",
        dest="account_mapping_translate_file",
        type=str,
        default="",
        help="Path to the account mapping translation file",
    )
    parser.add_argument(
        "-r",
        "--max-rows",
        dest="max_rows",
        type=int,
        default=MAX_ROW_DEFAULT,
        help=f"Maximum number of rows to process (default: {MAX_ROW_DEFAULT})",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    main(
        input_file=args.input_file,
        output_dir=args.output_dir,
        account_mapping_helper_file=args.account_mapping_helper_file,
        account_mapping_translate_file=args.account_mapping_translate_file,
        max_rows=args.max_rows,
    )

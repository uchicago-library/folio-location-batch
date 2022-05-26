import argparse
import configparser
import csv
import logging
import sys
from datetime import datetime, timezone

from folioclient import FolioClient
import json


def error_exit(status, msg):
    sys.stderr.write(msg)
    sys.exit(status)


def read_config(filename: str):
    """Parse the named config file and return an config object"""

    config = configparser.ConfigParser()
    try:
        config.read_file(open(filename))
    except FileNotFoundError as err:
        msg = f"{type(err).__name__}: {err}\n"
        error_exit(1, msg)
    except configparser.MissingSectionHeaderError as err:
        msg = f"{type(err).__name__}: {err}\n"
        error_exit(2, msg)
    return config


def parse_args():
    """Parse command line arguments and return a Namespace object."""

    parser = argparse.ArgumentParser(
        description="Update the fund code on purchase order lines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--infile",
        help="Input file (default: stdin)",
        default=sys.stdin,
        type=argparse.FileType("r"),
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="Output file (truncate if exists, default: stdout)",
        default=sys.stdout,
        type=argparse.FileType("w"),
    )
    parser.add_argument(
        "-C", "--config_file", help="Name of config file", default="config.ini"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity level"
    )
    parser.epilog = "In the input file, column 0 must contain the purchase order line no., and column 1 must contain the fund code."
    return parser.parse_args()


def get_pol_by_line_no(client: FolioClient, pol_no: str) -> dict:
    """
    Look up POL buy line number.

    Args:
        client: intialized FolioClient object
        pol_no: POL number

    Returns:
        A dictionary object containing the POL data.
    """
    path = "/orders/order-lines"
    query = f'?query=poLineNumber=="{pol_no}"'
    res = client.folio_get(path, None, query)

    if res["totalRecords"] == 0:
        return None
    elif res["totalRecords"] > 1:
        raise Exception(
            f'query for POL num {pol_no} resulted in {res["totalRecords"]} results, should be unique'
        )

    pol = res["poLines"][0]
    return pol


def write_result(out, output):
    """Placeholder for writing output"""
    out.write(output)


def main_loop(client, in_csv, out_csv):
    """
    Update the fund code for each POL in input.

    Iterates over the input file, assumes the POL number is in the first column
    and new fund code is in the second column.

    Writes an output row for each POL.

    Args:
    client: intialized FolioClient object
    in_csv: CSV reader object
    out_csv: CSV writer object
    """
    out_csv.writeheader()

    for row in in_csv:
        pol_no = row[0]
        fund = row[1]
        pol_id = None
        status = None
        msg = None
        # result = process_pol(client, pol_no, fund)
        pol = get_pol_by_line_no(client, pol_no)
        if pol is None:
            msg = f"No POL found for line number '{pol_no}'"
        else:
            # write_result(outfile, result)
            pol_id = pol["id"]
            pass

        out_csv.writerow(
            {
                "timestamp": datetime.now(timezone.utc),
                "pol_no": pol_no,
                "fund": fund,
                "status": status,
                "message": msg,
            }
        )


def main():
    args = parse_args()
    config = read_config(args.config_file)
    # Logic or function to override config values from the command line arguments would go here

    client = FolioClient(
        config["Okapi"]["okapi_url"],
        config["Okapi"]["tenant_id"],
        config["Okapi"]["username"],
        config["Okapi"]["password"],
    )

    fieldnames = ["timestamp", "pol_no", "fund", "pol_id", "status", "message"]
    main_loop(
        client,
        csv.reader(args.infile, dialect="excel-tab"),
        csv.DictWriter(args.outfile, fieldnames=fieldnames, dialect="excel-tab"),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)

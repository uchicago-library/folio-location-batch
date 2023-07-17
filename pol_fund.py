"""Update the funds in purchase order lines.

Use this to update funds on POLs, especially when moving encumbrances to different
funds as part of post-fiscal year rollover activity.

Workflow:
1. Look up POL by line number,
2. look up unreleased encumbrances,
3. release the encumbrances, and
4. recreate encumbrances of the same amounts on the new fund

Note: all entries in the fund distribution will be set to the same new fund. This
is a limitation of this automated process as when we give a specific new fund per
POL.
"""

##
# TODO:
#
# Clean up folioclient import lines
#
# Update FolioClient version and check for relevant get_* methods for financial data
#
# Remove dead code

import argparse
import configparser
import copy
import csv
import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone

import requests
from folioclient import FolioClient
from folioclient.FolioClient import FolioClient


def error_exit(status, msg):
    """Write out an error message and terminate with an exit status (convenience function)."""
    sys.stderr.write(msg)
    sys.exit(status)


def read_config(filename: str):
    """Parse the named config file and return an config object."""
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


def init_client(config):
    """Return an initialized client object.

    This small function is convenient when using the interactive interpreter.

    Args:
        config: ConfigParser object contianing config file data
    """
    return FolioClient(
        config["Okapi"]["okapi_url"],
        config["Okapi"]["tenant_id"],
        config["Okapi"]["username"],
        config["Okapi"]["password"],
    )


def parse_args():
    """Parse command line arguments and return a Namespace object."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--infile",
        help="input file (default: stdin)",
        default=sys.stdin,
        type=argparse.FileType("r"),
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="output file (truncate if exists, default: stdout)",
        default=sys.stdout,
        type=argparse.FileType("w"),
    )
    parser.add_argument(
        "-I",
        "--in_dialect",
        help="input CSV dialect (default: excel)",
        default="excel",
    )
    parser.add_argument(
        "-O",
        "--out_dialect",
        help="output CSV dialect (default: excel)",
        default="excel",
    )
    parser.add_argument(
        "-C", "--config_file", help="name of config file", default="config.ini"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity level"
    )
    parser.epilog = (
        "Input file column 0 must contain the PO line no., column 1 must contain the fund code.\n"
        + "\n"
        + "Input and output files can be in any dialect the csv parser class understands:\n"
        + "\t"
        + ", ".join(csv.list_dialects())
        + "\n"
        + "See https://docs.python.org/3/library/csv.html for more details"
    )

    return parser.parse_args()


def get_fiscal_year(client: FolioClient) -> dict:
    """
    Return current fiscal year as a dictionary.

    This implementation uses the Python date objects, which are naive about timezones and work at the full day level.
    FY start and end are formatted as full ISO dates, but seem to apply to the whole day.
    Looking at real data from Lotus, observe that the end of FY2022 is a full 24 hour before the start of FY2023:

    FY2022:
    "periodStart": "2021-07-01T05:00:00.000+00:00",
    "periodEnd": "2022-06-25T00:00:00.000+00:00",

    FY2023:
    "periodStart": "2022-06-26T00:00:00.000+00:00",
    "periodEnd": "2023-06-30T00:00:00.000+00:00",

    Args:
        client: intialized FolioClient object

    Returns:
        A dictionary containing the current fiscal year, None if there is no fiscal year covering the current date.
    """
    today = date.today()
    fyList = client.folio_get("/finance/fiscal-years")["fiscalYears"]
    for fy in fyList:
        start = datetime.fromisoformat(fy["periodStart"]).date()
        end = datetime.fromisoformat(fy["periodEnd"]).date()
        if start <= today and today <= end:
            return fy

    return None


def get_funds(client: FolioClient) -> dict:
    """
    Return a dictionary of all funds, indexed by fund code.

    Args:
        client: intialized FolioClient object
    """
    funds = {}
    for f in client.get_all("/finance/funds", "funds"):
        funds[f["code"]] = f
    return funds


def get_pol_by_line_no(client: FolioClient, pol_no: str) -> dict:
    """
    Look up POL by line number.

    Args:
        client: intialized FolioClient object
        pol_no: POL number

    Returns:
        A dictionary object containing the POL data.

    Raises:
        Exception if more than one POL matches the pol_no.
    """
    path = "/orders/order-lines"
    query = f'?query=poLineNumber=="{pol_no}"'
    res = client.folio_get(path, None, query)

    if res["totalRecords"] == 0:
        return None
    if res["totalRecords"] > 1:
        raise Exception(
            f'query for POL num {pol_no} resulted in {res["totalRecords"]} results, should be unique'
        )

    pol = res["poLines"][0]
    return pol


def get_encumbrances(client: FolioClient, pol_id: str, fy_id: str) -> list:
    """Look up all encumbrances for a POL in a fiscal year.

    Args:
        client: intialized FolioClient object
        pol_id: POL UUID
        fy_id:  fiscal year UUID

    Returns:
        A list object containing the transaction data.
    """
    enc_result = client.folio_get(
        "/finance-storage/transactions",
        query=f"?query=(encumbrance.sourcePoLineId={pol_id} and fiscalYearId={fy_id})",
    )
    return enc_result["transactions"]


def set_pol_fund(
    client: FolioClient,
    pol: dict,
    fund_code: str,
    funds: dict,
    fiscal_year: dict,
    verbose: bool,
    err_fp,
) -> tuple[str, str, str]:
    """
    Set the fund for the POL, release encumbrance on old fund and re-encumber on new fund.

    If there is more than one fund distribution, this will update all fund distributions to the new fund

    Args:
        client: intialized FolioClient object
        pol: purchase order line as dictionary
        fund_code: new fund_code code to assign
        funds: dictionary of funds indexed by code
        fiscal_year: current fiscal year as dictionary

    Returns:
        Tuple of HTTP status code, plus message and original fund distribution list if error.
    """
    pol_path = f"/orders/order-lines/{pol['id']}"
    pol_url = f"{client.okapi_url}/orders/order-lines/{pol['id']}"

    # TODO: release old encumbrances

    fundDistList = pol["fundDistribution"]
    fundDistListOrig = copy.deepcopy(fundDistList)

    # Identify the current Fiscal Year

    if verbose:
        err_fp.write("original POL fund dist:\n")
        json.dump(pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND original POL fund dist:\n")

    # Encumber on the new fund
    for fdist in fundDistList:
        fdist["code"] = fund_code
        fdist["fundId"] = funds[fund_code]["id"]
        # setting new encumbrance ID causes an a new encumbrance to be created on the fund
        fdist["encumbrance"] = str(uuid.uuid4())

    if verbose:
        err_fp.write("updated POL fund dist:\n")
        json.dump(pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND updated POL fund dist:\n")
    resp = requests.put(pol_url, headers=client.okapi_headers, data=json.dumps(pol))

    if verbose:
        err_fp.write(pol_url + "\n")
        err_fp.write(f"status = {resp.status_code};\ntext = {resp.text}\n")
        err_fp.write(pol_url + "\n")

    # Check updated POL...
    updated_pol = client.folio_get(pol_path)
    if verbose:
        err_fp.write("updated POL fund dist:\n")
        json.dump(updated_pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND updated POL fund dist:\n")

    # ... and return the update results if the check is good

    return (resp.status_code, resp.text, json.dumps(fundDistListOrig))


def reset_fund_dist(
    client: FolioClient, fundDist, fund_code: str, funds: dict
) -> tuple[str, str]:
    """
    Update all fund_code distributions.

    For each fund_code distribution, first release the encumbrance.
    """
    status_code = None
    msg = None

    for fdist in fundDist:
        # release current encumbrance
        release_url = f"/finance/release-encumbrance/{fdist['encumbrance']}"
        r = requests.post(release_url, headers=client.okapi_headers)
        status_code = r.status_code
        msg = r.text
        if status_code != "204":
            return (status_code, json.dumps(msg))

        new_fdist = fdist.copy()
        new_fdist["code"] = fund_code
        new_fdist["fundID"] = funds[code]["id"]
        pop(new_fdist, "encumbrance", None)
        # new_fdist["reEncumber"] = "true"

        # re-encumber to new fund_code code
        pass
    return (status_code, msg)


def write_result(out, output):
    """Write output (placeholder function)."""
    out.write(output)


def main_loop(client, in_csv, out_csv, verbose: bool, err_fp):
    """
    Update the fund code for each POL in input.

    Iterates over the input file, assumes the POL number is in the first column
    and new fund code is in the second column.

    Writes an output row for each POL.

    Args:
    client: initialized FolioClient object
    in_csv: CSV reader object
    out_csv: CSV writer object
    verbose: enable more diagnostic messages to the error output
    err_fp: file pointer for error messages
    """
    funds = get_funds(client)
    fiscal_year = get_fiscal_year(client)

    out_csv.writeheader()

    for row in in_csv:
        pol_no = row[0]
        fund = row[1]
        pol_id = None
        status_code = None
        msg = None

        # check whether the new fund code actually exists, report error and move on if it does not.
        if funds.get(fund) is None:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
                    "fund": fund,
                    "message": "fund code does not exist",
                    "manual_review": "Y",
                }
            )
            continue

        # result = process_pol(client, pol_no, fund)
        pol = get_pol_by_line_no(client, pol_no)

        if pol is None:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
                    "fund": fund,
                    "message": f"No POL found for line number '{pol_no}'",
                    "manual_review": "Y",
                }
            )
            continue

        if pol.get("fundDistribution") is None or len(pol["fundDistribution"]) == 0:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
                    "message": "POL has 0 fund distributions",
                    "manual_review": "Y",
                }
            )
            continue
        # Check if there is more than one fund distribution, report for manual review if so
        if len(pol["fundDistribution"]) > 1:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
                    "message": f"POL has {len(pol['fundDistribution'])} fund distributions",
                    "manual_review": "Y",
                }
            )
            continue

        #
        # Get encumbrances on this POL from this Fiscal Year and release
        #

        # enc_list = get_encumbrances(client, pol['id'], fiscal_year['id'])
        enc_list = client.folio_get(
            "/finance-storage/transactions",
            key="transactions",
            query=f"?query=(encumbrance.sourcePoLineId={pol['id']} and fiscalYearId={fiscal_year['id']} and encumbrance.status=Unreleased)",
        )
        if len(enc_list) != 1:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
                    "message": f"POL has {len(enc_list)} unreleased encumbrances",
                    "manual_review": "Y",
                }
            )
            continue
        for enc in enc_list:
            resp = requests.post(
                client.okapi_url + f"/finance/release-encumbrance/{enc['id']}",
                json={"id": enc["id"]},
                headers=client.okapi_headers,
            )
            if resp.status_code != 204:
                out_csv.writerow(
                    {
                        "timestamp": datetime.now(timezone.utc),
                        "pol_no": pol_no,
                        "status_code": resp.status_code,
                        "message": "failed to release encumbrance: "
                        + json.dumps(resp.text),
                        "manual_review": "Y",
                    }
                )
                continue

        # This code is from when we thought we would have to update fund distributions individually.
        # Now it looks like the /orders/order-lines API takes care of this in the business logic.
        # Remove this code when we confirm.
        if False:
            fundDist = pol["fundDistribution"]
            (status_code, msg) = reset_fund_dist(client, fundDist, fund)
            if status_code == "204":
                (status_code, msg) = set_pol_fund(client, pol, fund)
            pass

        (status_code, msg, fundDistOrig) = set_pol_fund(
            client, pol, fund, funds, fiscal_year, verbose, err_fp
        )

        out_csv.writerow(
            {
                "timestamp": datetime.now(timezone.utc),
                "pol_no": pol_no,
                "fund": fund,
                "pol_id": pol["id"],
                "status_code": status_code,
                "message": msg,
                "original_fund_distribution": fundDistOrig,
                "manual_review": "N",
            }
        )


def main():
    """Read command line arguments and config file and call main loop."""
    verbose = False
    args = parse_args()
    config = read_config(args.config_file)
    # Logic or function to override config values from the command line arguments would go here

    client = init_client(config)

    fieldnames = [
        "timestamp",
        "pol_no",
        "fund",
        "pol_id",
        "status_code",
        "message",
        "original_fund_distribution",
        "manual_review",
    ]
    main_loop(
        client,
        csv.reader(args.infile, dialect=args.in_dialect),
        csv.DictWriter(args.outfile, fieldnames=fieldnames, dialect=args.out_dialect),
        verbose,
        sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)

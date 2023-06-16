"""Reset expense classes on purchase order lines.

Workflow:
1. retrieve the POL and save the old fund distribution in memory
2. delete the fund distribution from the POL and save the POL, this will delete the old transactions
3. Put new expense class ids in the fund distribution that was saved in memory, re-add to the POL and save. This should trigger new encumbrance transaction, keep the same funds and encumbrances, etc.
"""

# feature branch


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
    """Convenience function to write out an error message and terminate with an exit status."""
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


def init_client(config):
    """Returns an initialized client object

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
        description="Reset expense classes on purchase order lines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-D",
        "--dump_expense_classes",
        help="Read and print out expense classes.",
        action="store_true",
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
    )
    parser.add_argument(
        "-I",
        "--in_dialect",
        help="input dialect (default: excel)",
        default="excel",
    )
    parser.add_argument(
        "-O",
        "--out_dialect",
        help="output dialect (default: excel)",
        default="excel",
    )
    parser.add_argument(
        "-C", "--config_file", help="Name of config file", default="config.ini"
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


def get_fiscal_year(client: FolioClient, code: str) -> dict:
    """
    Look up fiscal year by code.
    Args:
        client: intialized FolioClient object
        code: code for the desired fiscal year
    Returns:
        A dictionary containing the current fiscal year, None if there is no fiscal year covering the current date.
    """
    # TODO: modify query to only return the needed fiscal year (can't seem to get this right)
    fyList = client.folio_get("/finance/fiscal-years")["fiscalYears"]
    for fy in fyList:
        if fy["code"] == code:
            return fy

    return None


def get_expense_classes(client: FolioClient) -> list:
    """
    Returns a list of all expense class objects
    Args:
        client: intialized FolioClient object
    """
    exp_classes = []
    exp_classes.extend(
        client.get_all("/finance/expense-classes", "expenseClasses")
    )
    return exp_classes


def dump_expense_classes(expense_classes: list, file = sys.stdout):
    """
    Write out a human-readable summary of the expense classes.
    Args:
        expense_classes: dictionary of expense classes, indexed by "code"
        file:            text file to write to (default: stdout)
    """
    for ec in expense_classes:
        file.write(f'{ec["id"]}\t{ec["code"]}\t{ec["name"]}\n')


def get_funds(client: FolioClient) -> dict:
    """
    Returns a dictionary of all funds, indexed by fund code
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
    enc_result = client.folio_get(
        "/finance-storage/transactions",
        query=f"?query=(encumbrance.sourcePoLineId={pol_id} and fiscalYearId={fy_id})",
    )
    return enc_result["transactions"]


def reencumber_pol(
    client: FolioClient,
    pol: dict,
    verbose: bool,
    err_fp,
) -> tuple[str, str, str]:
    """
    Set the fund for the POL, release encumbrance on old fund and re-encumber on new fund.
    If there is more than one fund distribution, this will update all fund distributions to the new fund
    Args:
        client: intialized FolioClient object
        pol: purchase order line as dictionary
        verbose: enable more diagnostic messages to the error output
        err_fp: file pointer for error messages
    Returns:
        Tuple of HTTP status code, plus message and original fund distribution list if error.
    """
    pol_path = f"/orders/order-lines/{pol['id']}"
    pol_url = f"{client.okapi_url}/orders/order-lines/{pol['id']}"

    my_pol = copy.deepcopy(pol)
    fundDistList = pol["fundDistribution"]
    fundDistListOrig = copy.deepcopy(fundDistList)

    if verbose:
        err_fp.write("original POL fund dist:\n")
        json.dump(pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND original POL fund dist:\n")

    # delete fundDistribution
    my_pol.pop("fundDistribution")
    resp = requests.put(pol_url, headers=client.okapi_headers, data=json.dumps(my_pol))
    if verbose:
        err_fp.write(pol_url + "\n")
        err_fp.write(
            f"Delete fundDistribution:\nstatus = {resp.status_code};\ntext = {resp.text}\n"
        )
        err_fp.write(pol_url + "\n")
    if resp.status_code != 204:
        return (
            resp.status_code,
            "failed to remove fund distribution: \n" + resp.text,
            json.dumps(fundDistListOrig),
        )

    # Reencumber
    for fdist in fundDistList:
        # setting new encumbrance ID causes an a new encumbrance to be created on the fund
        fdist["encumbrance"] = str(uuid.uuid4())
    my_pol["fundDistribution"] = fundDistList
    if verbose:
        err_fp.write("updated POL fund dist:\n")
        json.dump(my_pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND updated POL fund dist:\n")
    resp = requests.put(pol_url, headers=client.okapi_headers, data=json.dumps(my_pol))

    if verbose:
        err_fp.write(pol_url + "\n")
        err_fp.write(f"status = {resp.status_code};\ntext = {resp.text}\n")
        err_fp.write(pol_url + "\n")

    # Check updated POL...
    if verbose:
        updated_pol = client.folio_get(pol_path)
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


def update_expense_class(
    client: FolioClient,
    pol: dict,
    exp_class_id: str,
    verbose: bool,
    err_fp,
) -> tuple[str, str, str]:
    """
    Set the expense class for the POL, release encumbrance on old expense class and re-encumber on new expense class.
    If there is more than one fund distribution, this will update all fund distributions to the new expense class.
    Args:
        client: intialized FolioClient object
        pol: purchase order line as dictionary
        exp_class_id: UUID of the new expense class
        verbose: enable more diagnostic messages to the error output
        err_fp: file pointer for error messages
    Returns:
        Tuple of HTTP status code, plus message and original fund distribution list if error.
    """
    pol_path = f"/orders/order-lines/{pol['id']}"
    pol_url = f"{client.okapi_url}/orders/order-lines/{pol['id']}"

    my_pol = copy.deepcopy(pol)
    fundDistList = pol["fundDistribution"]
    fundDistListOrig = copy.deepcopy(fundDistList)

    if verbose:
        err_fp.write("original POL fund dist:\n")
        json.dump(pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND original POL fund dist:\n")

    # delete fundDistribution
    my_pol.pop("fundDistribution")
    resp = requests.put(pol_url, headers=client.okapi_headers, data=json.dumps(my_pol))
    if verbose:
        err_fp.write(pol_url + "\n")
        err_fp.write(
            f"Delete fundDistribution:\nstatus = {resp.status_code};\ntext = {resp.text}\n"
        )
        err_fp.write(pol_url + "\n")
    if resp.status_code != 204:
        return (
            resp.status_code,
            "failed to remove fund distribution: \n" + resp.text,
            json.dumps(fundDistListOrig),
        )

    # Reencumber
    for fdist in fundDistList:
        # setting new encumbrance ID causes a new encumbrance to be created with the updated expense class
        fdist["encumbrance"] = str(uuid.uuid4())
        fdist["expenseClassId"] = exp_class_id
    my_pol["fundDistribution"] = fundDistList
    if verbose:
        err_fp.write("updated POL fund dist:\n")
        json.dump(my_pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND updated POL fund dist:\n")
    resp = requests.put(pol_url, headers=client.okapi_headers, data=json.dumps(my_pol))

    if verbose:
        err_fp.write(pol_url + "\n")
        err_fp.write(f"status = {resp.status_code};\ntext = {resp.text}\n")
        err_fp.write(pol_url + "\n")

    # Check updated POL...
    if verbose:
        updated_pol = client.folio_get(pol_path)
        err_fp.write("updated POL fund dist:\n")
        json.dump(updated_pol["fundDistribution"], err_fp, indent=2)
        err_fp.write("\nEND updated POL fund dist:\n")

    # ... and return the update results if the check is good

    return (resp.status_code, resp.text, json.dumps(fundDistListOrig))


def write_result(out, output):
    """Placeholder for writing output"""
    out.write(output)


def main_loop(client, exp_classes: list, in_csv, out_csv, verbose: bool, err_fp):
    """
    Update the fund code for each POL in input.
    Iterates over the input file, assumes the POL number is in the first column
    and new fund code is in the second column.
    Writes an output row for each POL.
    Args:
    client: initialized FolioClient object
    exp_classes: expense classes
    in_csv: CSV reader object
    out_csv: CSV writer object
    verbose: enable more diagnostic messages to the error output
    err_fp: file pointer for error messages
    """
    #
    # Set up needed look-up dictionaries
    #
    ec_by_id = {}
    for ec in exp_classes:
        ec_by_id[ec["id"]] = ec
    ec_by_code = {}
    for ec in exp_classes:
        ec_by_code[ec["code"]] = ec
    ec_by_name = {}
    for ec in exp_classes:
        ec_by_name[ec["name"]] = ec

    out_csv.writeheader()

    for row in in_csv:
        pol_no = row[0]
        exp_class_code = row[1]
        pol_id = None
        status_code = None
        msg = None

        pol = get_pol_by_line_no(client, pol_no)

        if pol is None:
            out_csv.writerow(
                {
                    "timestamp": datetime.now(timezone.utc),
                    "pol_no": pol_no,
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

        # save fund(s) for logging output
        funds = []
        for fdist in pol["fundDistribution"]:
            funds.append(fdist["code"])
            
        (status_code, msg, fundDistOrig) = update_expense_class(
            client, pol, ec_by_code[exp_class_code], verbose, err_fp
        )
        print(fundDistOrig)
        print(type(fundDistOrig))
        #sys.exit(1)
        
        orig_exp_code = []
        orig_exp_name = []
        for fdist in json.loads(fundDistOrig):
            orig_exp_code.append(ec_by_id[fdist["expenseClassId"]]["code"])
            orig_exp_name.append(ec_by_id[fdist["expenseClassId"]]["name"])

        out_csv.writerow(
            {
                "timestamp": datetime.now(timezone.utc),
                "pol_no": pol_no,
                "expense_code": exp_class_code,
                "pol_id": pol["id"],
                "status_code": status_code,
                "message": msg,
                "original_expense_code": " ".join(orig_exp_code),
                "original_expense_name": " ".join(orig_exp_name),
                "manual_review": "N",
            }
        )


def main2():
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
        "original_expense_code",
        "original_expense_name",
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


def main():
    verbose = False
    args = parse_args()
    config = read_config(args.config_file)
    # Logic or function to override config values from the command line arguments would go here

    client = init_client(config)

    fieldnames = [
        "timestamp",
        "pol_no",
        "expense_code",
        "pol_id",
        "status_code",
        "message",
        "original_expense_code",
        "original_expense_name",
        "manual_review",
    ]

    expense_classes = get_expense_classes(client)

    if args.dump_expense_classes:
        dump_expense_classes(expense_classes)
        sys.exit(0)

    main_loop(
        client,
        expense_classes,
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

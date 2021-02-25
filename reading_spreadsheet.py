import pandas as pd
from dateutil import parser
import openpyxl


def fixing_bad_column_return_none(column):
    if pd.isnull(column) or "--" in str(column):
        return None
    elif column == ' ' or column == "-":
        return None
    elif column == "N/A":
        return None
    else:
        return column


def publication_date(year, month, day):
    if not pd.isnull(year) and not pd.isnull(month) and not isinstance(month, str):
        spreadsheet_date = parser.parse("{}-{}-{}".format(year, int(month), day)).strftime("%Y-%m-%d")
    elif pd.isnull(month) and not pd.isnull(year):
        return year
    else:
        spreadsheet_date = None
    return spreadsheet_date


def corresponding_types_of_science(science_type):
    arr1 = []
    all_science_types = {"exg": "Extragalactic", "sne": "Supernovae and GrW", "hz": "high z",
                         "gal": "galaxy", "exo": "exoplanet/host stars", "bin": "binary",
                         "He": "Helium", "xrb": "X/gamma-ray binaries", "Gal": "Gal object",
                         "sol": "Solar system", "gw": "gravitational waves", "dwM": "M-dwarfs",
                         "cos": "cosmology", "nea": "near-earth asteroids", "sb": "starburst",
                         "psr": "pulsar", "cl": "cluster", "r-gal": "radio-gal", "ism": "interstellar matter",
                         "loc": "local neighbourhood (Gal: sun; exg: loc univ)",
                         "lss": "lss and distance measurements", "V*": "variable star",
                         "WR*": "Wolf-Rayet star", "agn": "active galactic nucleus"}
    correspondence = ""

    if not pd.isnull(science_type):
        for key, value in all_science_types.items():
            if key in science_type:
                arr1.append(value)
                correspondence = "-".join(arr1)
            if "--" in science_type:
                correspondence = None
    return correspondence


def create_dataframe(min_row, max_col, max_row):
    workbook = openpyxl.load_workbook("/home/lonwabolap/Downloads/SALT publication statistics.xlsx")
    sheet = workbook.active
    arr = []
    colors_to_avoid = ["FFB7B7B7", "FF999999"]
    for row in sheet.iter_rows(min_row=min_row, max_col=max_col, max_row=max_row):
        for cell in row:
            if cell.font.color is not None and cell.font.color.rgb in colors_to_avoid:
                arr.append(row[0].row)

    rows_to_be_removed = list(dict.fromkeys(arr))
    for i in rows_to_be_removed:
        sheet.delete_rows(i - rows_to_be_removed.index(i), 1)
    return wb


filepath = "/home/lonwabolap/Downloads/SALT publication statistics.xlsx"
spreadsheet = pd.read_excel(filepath, "Sheet1")
wb = create_dataframe(1, spreadsheet.shape[-1], spreadsheet.shape[0])
ws = wb.active
data = ws.values
cols = next(data)[0:]


def read_spreadsheet():
    df = pd.DataFrame(data, columns=cols)

    arr = []
    for index, row in df.iterrows():

        if not (pd.isnull(row["1st Author (status 1 November 2019)"]) or
                row["1st Author (status 1 November 2019)"].strip() == "") and \
                "--" not in row["1st Author (status 1 November 2019)"]:

            arr.append({

                "Name": row["1st Author (status 1 November 2019)"],

                "ADS link": row["ADS link"],

                "Institute of 1st author": fixing_bad_column_return_none(row["Institute of 1st author"]),

                "Position of 1st author": fixing_bad_column_return_none(row["Position of 1st author"]),

                "Partnership of 1st author": fixing_bad_column_return_none(row["Partnership of 1st author"]),

                "Proposal code(s)": [{
                    "Proposal code": fixing_bad_column_return_none(row["Proposal code(s)"]),
                    "semester": fixing_bad_column_return_none(row["Proposal semester"]),
                    "ToO":fixing_bad_column_return_none(row["ToO"]),
                    "PI": fixing_bad_column_return_none(row["PI"]),
                    "Partner(time allocated)": fixing_bad_column_return_none(row["Partner (time allocated)"]),
                    "Institutes (on proposal)": fixing_bad_column_return_none(row["Institutes (on proposal)"]),
                    "student project": fixing_bad_column_return_none(row["student project"]),
                    "Instrument(s)": fixing_bad_column_return_none(row["Instrument(s)"]),
                    "instrument mode(s)": fixing_bad_column_return_none(row["Instrument mode(s)"]),
                    "observation date": fixing_bad_column_return_none(row["Dates obs (cf WM)"]),
                    "Priorities": fixing_bad_column_return_none(row["Priority (ies)"]),
                    "Total SALT time": fixing_bad_column_return_none(row["Total SALT time"]),
                    "Fraction of total time": fixing_bad_column_return_none(row["Fraction of total time [%]"])
                }],

                "Publication Paper": [{"Publication date": publication_date(row["Year"],
                                                                            row["Month (the ADS 'pub date')"], 15),
                                       "Full author list": fixing_bad_column_return_none(row["Full author list"]),
                                       "Partners (on paper)": fixing_bad_column_return_none(
                                           row["Partners (on paper, excl 1st author)"]),
                                       "Institutes of partners (on paper, excl 1st author)":
                                           fixing_bad_column_return_none
                                           (row["Institutes of partners (on paper, excl 1st author)"]),
                                       "No of SA": fixing_bad_column_return_none(row["Num of SA on paper"]),
                                       "Comments": fixing_bad_column_return_none(row["Comments"]),
                                       "No of papers": fixing_bad_column_return_none(row["Number of papers"]),
                                       "Type of paper": fixing_bad_column_return_none(row["type of paper"]),
                                       "Type of Science": corresponding_types_of_science(row["type of science"])
                                       }]

            })

        if pd.isnull(row["1st Author (status 1 November 2019)"]) or \
                row["1st Author (status 1 November 2019)"].strip() == "":
            arr[-1]["Proposal code(s)"].append({
                "Proposal code": fixing_bad_column_return_none(row["Proposal code(s)"]),
                "semester": fixing_bad_column_return_none(row["Proposal semester"]),
                "ToO": fixing_bad_column_return_none(row["ToO"]),
                "PI": fixing_bad_column_return_none(row["PI"]),
                "Partner(time allocated)": fixing_bad_column_return_none(row["Partner (time allocated)"]),
                "Institutes (on proposal)": fixing_bad_column_return_none(row["Institutes (on proposal)"]),
                "student project": fixing_bad_column_return_none(row["student project"]),
                "Instrument(s)": fixing_bad_column_return_none(row["Instrument(s)"]),
                "Instrument mode(s)": fixing_bad_column_return_none(row["Instrument mode(s)"]),
                "Observation date": fixing_bad_column_return_none(row["Dates obs (cf WM)"]),
                "Priorities": fixing_bad_column_return_none(row["Priority (ies)"]),
                "Total SALT time": fixing_bad_column_return_none(row["Total SALT time"]),
                "Fraction of total time": fixing_bad_column_return_none(row["Fraction of total time [%]"])
            })

    return arr

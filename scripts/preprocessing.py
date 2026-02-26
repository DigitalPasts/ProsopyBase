"""
preprocessing.py

Cleans and normalizes the ProsoBAB raw export to produce a standardized dataset
ready for downstream analysis (date estimation, co-witness network analysis, etc.).

Pipeline:
    1. Load raw data from data/raw/initial_prosobab_data.csv
    2. Structural normalization:
       - Fill missing Julian date entries
       - Extract the primary Julian year from split-year entries (e.g. '517/516' -> '517')
       - Remove attestations without a valid PID
    3. Apply documented error corrections (see error_correction.py)
    4. Write output to data/processed/preprocessed_whole_data.csv

Decisions tracking:
    - Split years (e.g. 517/516) are interpreted as the left (earlier) year.
    - People with attested dates do not receive estimations downstream.
    - Duplicate attestations are caused by differences in the 'Type and objects' column,
      which is not relevant for prosopographical analysis; they are not treated as errors.
"""

import pandas as pd

from helpers import get_fully_dated_rows_by_julian
from error_correction import apply_corrections

raw_data_path = '../data/raw/initial_prosobab_data.csv'
processed_data_path = '../data/processed/preprocessed_whole_data.csv'
corrections_path = '../data/corrections/final-fixes.csv'


def dup_attestations(df):
    """
    Investigates duplicate attestations (attestations should be unique).
    Duplicate attestations are due to differences in 'types and objects column',
    which is not relevant for this task. All tablets with the same tablet_id have
    the same year, which is what matters.

    Saves results to data/corrections/duplicate_attestations_from_initial.csv.
    """
    data_df = df

    df_filtered = data_df[data_df["PID"] != "-"]
    duplicate_rows = df_filtered[df_filtered.duplicated(subset=["Attestation ID"], keep=False)]
    identical_duplicates = duplicate_rows[duplicate_rows.duplicated(keep=False)]
    non_identical_duplicates = duplicate_rows.drop(identical_duplicates.index)
    significant_differences = non_identical_duplicates.groupby("Attestation ID").filter(
        lambda group: not group.drop(columns=["Type and objects"]).duplicated(keep=False).all()
    )
    assert(len(significant_differences) == 0)

    tablets_df = data_df.groupby('Tablet ID')
    mixed_tablets = []
    for tablet_id, tablet_rows in tablets_df:
        if tablet_rows['Julian date'].nunique() > 1:
            mixed_tablets.append(tablet_id)

    assert(len(mixed_tablets) == 0)
    return data_df


def verify_julian_date(data_df):
    """
    Calculates the expected Julian date from the Babylonian date, known king years,
    and a simple formula. Inconsistencies are reported in
    data/corrections/julian_dates_calculator_mismatches.csv and are not corrected in place.

    :param data_df: the raw df of prosobab
    """
    verification_df = pd.read_csv('../data/corrections/unique_kings_verification.csv')
    data_df = get_fully_dated_rows_by_julian(data_df)
    data_df = data_df.astype({'PID': int, 'Split_Julian_dates': int, 'Tablet ID': int})

    tablets_df = data_df.groupby('Tablet ID')
    mismatches = []
    for tab_id, tablet in tablets_df:
        king_matches = tablet.loc[tablet['Role'].str.contains('king in'), 'PID'].values
        king_pid = king_matches[0] if len(king_matches) > 0 else None
        if king_pid is None:
            continue
        julian_in_data = int(tablet['Split_Julian_dates'].tolist()[0])
        baby_date = tablet['Date'].tolist()[0]

        day, month, rest = baby_date.strip().split('.')
        regnal_year_str, *ruler_parts = rest.strip().split()
        if not regnal_year_str.isdigit():
            continue
        regnal_year = int(regnal_year_str)
        start_year = verification_df.loc[verification_df['PID'] == king_pid, 'start_year'].values[0]
        calculated_julian_year = int(start_year - (regnal_year - 1))
        print(tab_id, baby_date, "king start: ", start_year, "calculated julian: ",
              calculated_julian_year, "julian in data: ", julian_in_data)

        absolute_difference = abs(calculated_julian_year - julian_in_data)
        if absolute_difference > 1:
            mismatches.append([tab_id, baby_date, start_year, calculated_julian_year, julian_in_data])

    mismatches_df = pd.DataFrame(
        mismatches,
        columns=[
            'Tablet ID',
            'Babylonian Date',
            'King Start Year',
            'Calculated Julian Year',
            'Julian Year in Data'
        ]
    )
    mismatches_df.to_csv('../data/corrections/julian_dates_calculator_mismatches.csv', index=False)


def inconsistencies_by_king(data_df):
    """
    Tracks inconsistencies related to the Julian year based on pre-defined king years
    (ruling data taken from eBL). Compares known ruling years to the dataset to uncover
    severe errors (mostly typos, e.g. 51 instead of 531). Reported in
    data/corrections/initial_prosobab_inconsistencies_julian_date.csv; not corrected in place.
    """
    data_df = get_fully_dated_rows_by_julian(data_df)
    data_df = data_df.astype({'PID': int, 'Split_Julian_dates': int, 'Tablet ID': int})
    verification_df = pd.read_csv('../data/corrections/unique_kings_verification.csv')

    verified_kings_pids = verification_df['PID'].tolist()
    inconsistencies = []

    tablets_df = data_df.groupby('Tablet ID')
    for tab_id, tablet in tablets_df:
        jul_date = tablet['Split_Julian_dates'].unique()[0]
        king_matches = tablet.loc[tablet['Role'].str.contains('king in'), 'PID'].values
        king_pid = king_matches[0] if len(king_matches) > 0 else None
        if king_pid is None:
            continue
        elif king_pid and king_pid not in verified_kings_pids:
            print("Unrecognized king PID: ", king_pid)
            continue
        else:
            king_start = verification_df.loc[verification_df['PID'] == king_pid, 'start_year'].values[0]
            king_end = verification_df.loc[verification_df['PID'] == king_pid, 'end_year'].values[0]
            if int(jul_date) > int(king_start) + 1:
                diff = int(jul_date) - int(king_start)
                inconsistencies.append({
                    'tablet_id': tab_id,
                    'julian_date': int(jul_date),
                    'king_pid': king_pid,
                    'king_start_year': king_start,
                    'king_end_year': king_end,
                    'difference': diff,
                    'issue_type': 'before_start'
                })
            if int(jul_date) + 1 < int(king_end):
                diff = int(king_end) - int(jul_date)
                inconsistencies.append({
                    'tablet_id': tab_id,
                    'julian_date': int(jul_date),
                    'king_pid': king_pid,
                    'king_start_year': king_start,
                    'king_end_year': king_end,
                    'difference': diff,
                    'issue_type': 'after_end'
                })

    inconsistencies_df = pd.DataFrame(inconsistencies)
    inconsistencies_df.to_csv(
        '../data/corrections/initial_prosobab_inconsistencies_julian_date.csv', index=False
    )


def preprocess():
    """
    Main preprocessing pipeline. Reads the raw ProsoBAB export, normalizes it,
    applies documented corrections, and writes the result to
    data/processed/preprocessed_whole_data.csv.
    """
    df = pd.read_csv(raw_data_path).copy()

    # structural normalization: fill missing dates, extract primary Julian year, filter PIDs
    df['Julian date'] = df['Julian date'].fillna('-')
    df['Split_Julian_dates'] = df['Julian date'].str.split(pat="/").str[0]
    df['Split_Julian_dates'] = df['Split_Julian_dates'].str.extract(r'^(\d{3,4})$')
    df = df[df['PID'].str.isdigit()].copy()

    # apply documented date corrections (see data/corrections/final-fixes.csv)
    # diagnostic checks (commented out - run manually to regenerate diagnostic outputs):
    # dup_attestations(df)
    # inconsistencies_by_king(df)
    # verify_julian_date(df)
    df = apply_corrections(df, corrections_path)

    df.to_csv(processed_data_path, index=False)
    print(f"Preprocessed data written to {processed_data_path}")
    print(f"  Rows: {len(df)}")
    print(f"  Unique tablets: {df['Tablet ID'].nunique()}")
    print(f"  Unique people (PIDs): {df['PID'].nunique()}")


if __name__ == '__main__':
    preprocess()

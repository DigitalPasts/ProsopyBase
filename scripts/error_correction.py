"""
error_correction.py

Applies documented corrections to known date errors in the ProsoBAB raw export.
Corrections are recorded in data/corrections/final-fixes.csv.

Background:
    The ProsoBAB database contains a small number of tablets whose Julian year
    values are erroneous (typically typographic errors, e.g. '51' instead of '531').
    These errors were identified by cross-referencing known regnal year ranges against
    the attested dates (see data/corrections/initial_prosobab_inconsistencies_julian_date.csv
    and data/corrections/julian_dates_calculator_mismatches.csv for the diagnostic outputs
    that informed the corrections).

    The correction table (final-fixes.csv) records 25 tablet-level fixes:
        tablet_id  |  current_julian  |  change_to

Usage:
    from error_correction import apply_corrections
    df_corrected = apply_corrections(df)
"""

import pandas as pd


def apply_corrections(
    df: pd.DataFrame,
    corrections_path: str = '../data/corrections/final-fixes.csv'
) -> pd.DataFrame:
    """
    Apply documented date corrections to the Split_Julian_dates column.

    :param df: DataFrame with at least 'Tablet ID' and 'Split_Julian_dates' columns
    :param corrections_path: path to the corrections CSV (tablet_id, current_julian, change_to)
    :return: DataFrame with corrections applied
    """
    corrections_df = pd.read_csv(corrections_path)
    corrections_map = corrections_df.set_index('tablet_id')['change_to'].to_dict()
    df['Split_Julian_dates'] = df.apply(
        lambda row: corrections_map.get(row['Tablet ID'], row['Split_Julian_dates']),
        axis=1
    )
    return df

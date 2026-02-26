import statistics
import pandas as pd
from numpy import float64

def get_fully_dated_rows_by_julian(df):
  col = df['Split_Julian_dates']
  if col.dtype == object or col.dtype.name == "string":
    return df[df['Split_Julian_dates'].str.isdigit()].copy()
  else:
  # elif col.dtype == float64 or col.dtype == 'float64' or col.dtype == 'float32':
    return df[col.notna()].copy()


def get_dateless_rows(data_df):
  return data_df[~data_df['Split_Julian_dates'].fillna('').str.match(r'^\d{3}$')].copy()


def filter_missing_pids(df):
  return df[df['PID'].str.isdigit()].copy()


def get_avg_num_ppl(data_df):
  results = []
  tablets = data_df.groupby('Tablet ID')
  for name, group in tablets:
    results.append(len(group))

  return statistics.mean(results)


def get_most_popular_people(data_df):
  results = []
  people = data_df.groupby('PID')
  for name, group in people:
    results.append(len(group['Tablet ID'].unique()))

  results.sort(reverse=True)
  return results


def get_avg_num_docs_per_person(data_df):
  results = []
  ppl_groups = data_df.groupby('PID')
  for name, group in ppl_groups:
    results.append(len(group['Tablet ID'].unique()))

  return statistics.mean(results)

def convert_nabonassar_date_to_julian_year(date_str: str) -> int:
  """
  Convert a Babylonian Nabonassar date like '06.VIII.29 Dar I' to the Julian year.
  Returns the proleptic Julian year (negative values mean BCE).
  """
  # Mapping of rulers to their starting SE Babylonian year
  ruler_se_start = {
    "Dar I": 227,
    "Xerxes": 256,
    "Artaxerxes I": 276,
    "Darius II": 311,
    "Artaxerxes II": 332,
    "Artaxerxes III": 362,
    "Arses": 391,
    "Darius III": 393,
    # Add more if needed
  }

  # Babylonian month numerals to values
  month_map = {
    'I': 1, 'II': 2, 'III': 3, 'IV': 4,
    'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8,
    'IX': 9, 'X': 10, 'XI': 11, 'XII': 12,
    'VIb': 13, 'XIIb': 14  # intercalary months
  }

  # Parse input like '06.VIII.29 Dar I'
  try:
    day_part, month_part, rest = date_str.strip().split('.')
    regnal_year_str, *ruler_parts = rest.strip().split()
    regnal_year = int(regnal_year_str)
    ruler = ' '.join(ruler_parts)

    if ruler not in ruler_se_start:
      raise ValueError(f"Ruler '{ruler}' not in known list.")

    se_start = ruler_se_start[ruler]
    se_year = se_start + regnal_year - 1

    # Convert Seleucid Era Babylonian year to Julian year
    # SE year 1 = 311 BCE, so:
    julian_year = -311 + 1 + (se_year - 1)  # because SE year 1 = -310

    return julian_year

  except Exception as e:
    raise ValueError(f"Could not parse date string: {e}")



    #   # if king_start is None or king_end is None:
    #   #   print("No king start or king end for tablet id ", tab_id)
    #   #   continue
    #   print("jul_date: ", int(jul_date), "\nking start: ", int(king_start), "\nking end: ", int(king_end))



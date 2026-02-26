# ProsopyBase

Raw data, error correction, and preprocessing pipeline for the **ProsoBAB** Neo-Babylonian prosopography database.

This repository is the foundational data layer for the ProsopyEstimation and ProsopyWitness projects. It produces a clean, standardized attestation dataset (`preprocessed_whole_data.csv`) that downstream analyses build on.

---

## Repository Structure

```
ProsopyBase/
├── data/
│   ├── raw/
│   │   └── initial_prosobab_data.csv          # Raw export from ProsoBAB database
│   ├── corrections/
│   │   ├── final-fixes.csv                    # 25 documented date corrections
│   │   ├── unique_kings_verification.csv      # Known regnal year ranges (from eBL)
│   │   ├── duplicate_attestations_from_initial.csv
│   │   ├── julian_dates_calculator_mismatches.csv
│   │   └── initial_prosobab_inconsistencies_julian_date.csv
│   └── processed/
│       └── preprocessed_whole_data.csv        # Output: clean, corrected dataset
└── scripts/
    ├── helpers.py                             # Shared utility functions
    ├── error_correction.py                    # Applies documented date corrections
    └── preprocessing.py                       # Main preprocessing pipeline
```

---

## Data Description

### `data/raw/initial_prosobab_data.csv`
The raw export from the [ProsoBAB database](https://prosobab.leidenuniv.nl/) — a prosopography of individuals attested in Neo-Babylonian and Achaemenid-period legal documents (c. 625–330 BCE). Each row is one attestation of one person in one tablet.

Key columns: `PID`, `Tablet ID`, `Julian date`, `Date`, `Role`, `ind.Name`, `ind.Patronym`, `ind.Family name`, `Archive`, `Place of writing`

### `data/corrections/`
Results of the data-curation investigation that preceded regular preprocessing:

| File | Contents |
|---|---|
| `final-fixes.csv` | 25 tablet-level Julian year corrections (see below) |
| `unique_kings_verification.csv` | Ruling year ranges for each attested Babylonian king, sourced from eBL |
| `duplicate_attestations_from_initial.csv` | Diagnostic: attestations appearing more than once (explained by differing "Type and objects" values, not errors) |
| `julian_dates_calculator_mismatches.csv` | Diagnostic: tablets where the calculated Julian year (from regnal year + king start) differs from the recorded value |
| `initial_prosobab_inconsistencies_julian_date.csv` | Diagnostic: tablets whose Julian date falls outside the known reign of their attested king |

**About the corrections (`final-fixes.csv`)**: 25 tablets contain clear typographic errors in their Julian year (e.g. `51` instead of `531`). These were identified by cross-referencing king regnal ranges against attested dates. The corrections are applied programmatically by `error_correction.py`; the raw data file is never modified.

### `data/processed/preprocessed_whole_data.csv`
The output of the preprocessing pipeline:
- ~15,000+ attestations of ~11,500+ individuals across ~4,000 tablets
- Structural normalization applied (split-year entries resolved, PID-less rows removed)
- All 25 documented date corrections applied
- This file is the input for ProsopyEstimation and ProsopyWitness

---

## Preprocessing Pipeline

```
data/raw/initial_prosobab_data.csv
        |
        v
[preprocessing.py]
  1. Fill missing Julian date fields
  2. Extract primary year from split-year entries (e.g. "517/516" → "517")
  3. Remove attestations without a valid PID
        |
        v
[error_correction.py]
  Apply 25 documented corrections from data/corrections/final-fixes.csv
        |
        v
data/processed/preprocessed_whole_data.csv
```

---

## Requirements

```
pip install -r requirements.txt
```

Dependencies: `pandas`, `numpy`

---

## Usage

Run the full preprocessing pipeline from the `scripts/` directory:

```bash
cd scripts
python preprocessing.py
```

Output: `data/processed/preprocessed_whole_data.csv`

Expected output summary:
```
Preprocessed data written to ../data/processed/preprocessed_whole_data.csv
  Rows: ~15128
  Unique tablets: ~4005
  Unique people (PIDs): ~11496
```

To verify a specific correction (e.g. tablet 432, corrected from 527 → 536):
```python
import pandas as pd
df = pd.read_csv('data/processed/preprocessed_whole_data.csv')
print(df[df['Tablet ID'] == 432]['Split_Julian_dates'].unique())
# Expected: [536.0]
```

---

## Downstream Projects

- **ProsopyEstimation** — date estimation for undated tablets, using `preprocessed_whole_data.csv` as input
- **ProsopyWitness** — co-witness network analysis, using `preprocessed_whole_data.csv` as input

---

## Data Source

ProsoBAB: Prosopography of Babylonia, c. 625–330 BCE
[https://prosobab.leidenuniv.nl/](https://prosobab.leidenuniv.nl/)

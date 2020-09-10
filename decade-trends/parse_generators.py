import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

energy_source_mapping = {
    'NG': 'natural gas',
    'BIT': 'coal',
    'SUB': 'coal',
    'NUC': 'nuclear',
    'WAT': 'hydro',
    'WND': 'wind',
    'RFO': 'oil',
    'DFO': 'oil',
    'LIG': 'coal',
    'GEO': 'other', # geothermal
    'PC':  'oil', # petroleum coke
    'MSW': 'by-products', # muni solid waste
    'WDS': 'by-products', # wood solids
    'LFG': 'by-products', # landfill gas
    'WH': 'other', # waste heat
    'WC': 'coal', # waste coal
    'KER': 'oil',
    'SUN': 'solar',
    'SGC': "coal", # coal sythetic gas
    'JF': 'oil',  # jet fuel
    'OBL': 'by-products', # other biomass liquids
    'AB': 'by-products',  # ag by-products
    'WO': 'oil', # waste or other oil
    'TDF': 'by-products', # tires
    'OBS': 'by-products', # other biomass 
    'MWH': 'storage',
    'OBG': 'by-products', # other biomass gasses
    'OG': 'natural gas', # other natural gas
    # new between 2010 and 2018
    'SGP': 'oil', # synthetic gas from petroleum coke
    'RC': 'coal', # refined coal - damn there is a lot of this in 2018!!
    'OTH': 'other',
    'PG': 'oil', # gaseous propane
    # older (pre-2001) fuel types
    "COL": "coal",
    "PET": "oil",
    "UNK": "other",
    "GAS": "natural gas",
    "OO": "other",
    "WOC": "other",
    "REF": "by-products",
    "MF": "other",
}

def _get_file(year=2018):
    url_by_year = {
        2018: "https://www.eia.gov/electricity/data/eia860/xls/eia8602018.zip",
        2010: "https://www.eia.gov/electricity/data/eia860/archive/xls/eia8602010.zip",
        2000: "https://www.eia.gov/electricity/data/eia860/eia860a/eia860a2000.zip",
    }
    resp = urlopen(url_by_year[year])
    zipfile = ZipFile(BytesIO(resp.read()))
    fnm_gen = next(filter(lambda f: 'Generator' in f, zipfile.namelist()))
    if year <= 2000:
        fnm_gen = f"ExistingGenerators{year}.xls"
    return zipfile.open(fnm_gen)


def parse_generators(year=2018):
    columns = [
        "generator_uuid",
        "utility_name",
        "plant_name",
        "state",
        "county",
        "status",
        "sector",
        "nameplate_capacity",
        "energy_source_1",
        "operating_year",
        "planned_retirement_year",
    ]

    df = pd.read_excel(
        _get_file(year),
        sheet_name='Operable' if year == 2018 else "Exist",
        skiprows=1 if year == 2018 else 0,
    )
    df = df.rename(columns={
        c:
        c.lower() if year == 2010 else
        c.lower().replace(' ', '_').replace('?', '').replace('/', '_').split('_(')[0]
        for c in df.columns
    }).rename(columns={
        "nameplate": "nameplate_capacity",
        "sector_name": "sector",
        "sector": "sector_code",
    })
    df = df.assign(
        generator_uuid=df.plant_code.astype('str') + '_' +  df.generator_id
    )[columns].set_index("generator_uuid")
    assert(df.index.is_unique)
    df['planned_retirement_year'] = df.planned_retirement_year.replace(' ', np.nan).astype(float)
    df['energy_source'] = df.energy_source_1.replace(energy_source_mapping)
    return df[(df.status == 'OP') & df.sector.isin(['Electric Utility', 'IPP Non-CHP', 'IPP'])].copy()


def parse_proposed(year=2018):
    df_proposed = pd.read_excel(
        _get_file(year),
        sheet_name='Proposed',
        skiprows=1
    )
    df_proposed = df_proposed.rename(columns={
        c: c.lower().replace(' ', '_').replace('?', '').replace('/', '_').split('_(')[0]
        for c in df_proposed.columns
    }).rename(columns={
        "nameplate": "nameplate_capacity",
        "sector_name": "sector",
        "sector": "sector_code",
    })[[
        "utility_name",
        "plant_name",
        "plant_code",
        "state",
        "county",
        "nameplate_capacity",
        "status",
        "energy_source_1",
        "effective_year",
        "sector",
    ]]
    df_proposed = df_proposed[
        df_proposed.sector.isin(["IPP Non-CHP", "Electric Utility"]) &
        df_proposed.status.isin(["TS", "V", "U", "T"]) # must have regulatory approval or better 
    ].copy()
    df_proposed['energy_source'] = df_proposed.energy_source_1.replace(energy_source_mapping)  
    df_proposed.effective_year = df_proposed.effective_year.replace(' ', np.nan).astype(float)
    return df_proposed

def parse_retired(year=2018):
    df = pd.read_excel(
        _get_file(year),
        sheet_name='Retired and Canceled' if year > 2000 else "Existing Generators",
        skiprows=1 if year > 2000 else 0,
    )
    df = df.rename(columns={
        c: c.lower().replace(' ', '_').replace('?', '').replace('/', '_').split('_(')[0]
        for c in df.columns
    }).rename(columns={
        "sector_name": "sector",
        "sector": "sector_code",
    })
    if year <= 2000:
        df["sector"] = "Electric Utility"
        df = df.rename(columns={
            "operating_month": "operating_year",
            "operating_year": "operating_month",
            "retirement_month": "retirement_year",
            "retirement_year": "retirement_month",
            "existing_nameplate": "nameplate_capacity",
            "existing_energy_source_1": "energy_source_1",
            "existing_status": "status",
        })
        df["utility_name"] = ""
        df["plant_name"] = ""
        df["state"] = ""
        df["county"] = ""
        df.nameplate_capacity = df.nameplate_capacity / 1000
        
    df['energy_source'] = df.energy_source_1.replace(energy_source_mapping)
    df = df[
        df.sector.isin(["IPP Non-CHP", "Electric Utility"]) &
        (df.status == "RE")
    ].copy()[[
        "utility_name",
        "plant_name",
        "plant_code",
        "state",
        "county",
        "nameplate_capacity",
        "energy_source",
        "operating_year",
        "retirement_year",
    ]]
    for col in ['operating_year', 'retirement_year', 'nameplate_capacity']:
        df[col] = df[col].replace(' ', np.nan).round().astype(int)
    return df
import fnmatch
from dagster import asset
from minio import Minio
from io import BytesIO
import pandas as pd

def read_csv_from_minio():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    data = client.get_object("lakehouse", "raw/crm/NewCust.csv")
    column_names = [
        'action_ts', 'c_dob', 'c_gndr', 'c_id', 'c_tax_id', 'c_tier', 'c_l_name',
        'c_f_name', 'c_m_name', 'c_adline1', 'c_adline2', 'c_zipcode',
        'c_city', 'c_state_prov', 'c_ctry', 'c_prim_email', 'c_alt_email',
        'c_ctry_code_1', 'c_area_code_1', 'c_local_1', 'c_ctry_code_2',
        'c_area_code_2', 'c_local_2', 'c_ctry_code_3', 'c_area_code_3',
        'c_local_3', 'c_lcl_tx_id', 'c_nat_tx_id', 'ca_id', 'ca_tax_st',
        'ca_b_id', 'ca_name', 'card'
    ]

    df_nc = pd.read_csv(BytesIO(data.read()), header=None, names=column_names)
    df_nc = df_nc.where(pd.notnull(df_nc), None)

    # Add missing columns with NULL values
    df_nc['action_ts'] = pd.to_datetime(df_nc['action_ts'])
    df_nc['c_dob'] = pd.to_datetime(df_nc['c_dob'])
    df_nc['c_gndr'] = df_nc['c_gndr'].astype(str)
    df_nc['c_id'] = df_nc['c_id'].astype(int)
    df_nc['c_tax_id'] = df_nc['c_tax_id'].astype(str)
    df_nc['c_tier'] = df_nc['c_tier'].astype(str)
    df_nc['c_l_name'] = df_nc['c_l_name'].astype(str)
    df_nc['c_f_name'] = df_nc['c_f_name'].astype(str)
    df_nc['c_m_name'] = df_nc['c_m_name'].astype(str)
    df_nc['c_adline1'] = df_nc['c_adline1'].astype(str)
    df_nc['c_adline2'] = df_nc['c_adline2'].astype(str)
    df_nc['c_zipcode'] = df_nc['c_zipcode'].astype(str)
    df_nc['c_city'] = df_nc['c_city'].astype(str)
    df_nc['c_state_prov'] = df_nc['c_state_prov'].astype(str)
    df_nc['c_ctry'] = df_nc['c_ctry'].astype(str)
    df_nc['c_prim_email'] = df_nc['c_prim_email'].astype(str)
    df_nc['c_alt_email'] = df_nc['c_alt_email'].astype(str)
    df_nc['c_ctry_code_1'] = df_nc['c_ctry_code_1'].astype(str)
    df_nc['c_area_code_1'] = df_nc['c_area_code_1'].astype(str)
    df_nc['c_local_1'] = df_nc['c_local_1'].astype(str)
    df_nc['c_ctry_code_2'] = df_nc['c_ctry_code_2'].astype(str)
    df_nc['c_area_code_2'] = df_nc['c_area_code_2'].astype(str)
    df_nc['c_local_2'] = df_nc['c_local_2'].astype(str)
    df_nc['c_ctry_code_3'] = df_nc['c_ctry_code_3'].astype(str)
    df_nc['c_area_code_3'] = df_nc['c_area_code_3'].astype(str)
    df_nc['c_local_3'] = df_nc['c_local_3'].astype(str)
    df_nc['c_lcl_tx_id'] = df_nc['c_lcl_tx_id'].astype(str)
    df_nc['c_nat_tx_id'] = df_nc['c_nat_tx_id'].astype(str)
    df_nc['ca_id'] = df_nc['ca_id'].astype(int)
    df_nc['ca_tax_st'] = df_nc['ca_tax_st'].astype(int)
    df_nc['ca_b_id'] = df_nc['ca_b_id'].astype(int)
    df_nc['ca_name'] = df_nc['ca_name'].astype(str)
    df_nc['card'] = df_nc['card'].astype(str)
    df_nc['c_phone_1_ctry_code'] = None 
    df_nc['c_phone_1_area_code'] = None
    df_nc['c_phone_1_c_local'] = None
    df_nc['c_phone_2_ctry_code'] = None
    df_nc['c_phone_2_area_code'] = None
    df_nc['c_phone_2_c_local'] = None
    df_nc['c_phone_3_ctry_code'] = None
    df_nc['c_phone_3_area_code'] = None
    df_nc['c_phone_3_c_local'] = None 
    for col in df_nc.columns:
        df_nc[col] = df_nc[col].replace('nan', None)
    df_nc.replace('None', None, inplace=True)
    df_nc['action_type'] = 'NewCust'

############################ AddAcct ########################################
    data = client.get_object("lakehouse", "raw/crm/AddAcct.csv")
    column_names = ['action_ts', 'c_id', 'ca_id', 'ca_tax_st', 'ca_b_id', 'ca_name']

    df_aa = pd.read_csv(BytesIO(data.read()), header=None, names=column_names)
    df_aa = df_aa.where(pd.notnull(df_aa), None)

    # Adjust data types
    df_aa['action_ts'] = pd.to_datetime(df_aa['action_ts'])
    df_aa['c_id'] = df_aa['c_id'].astype(int)
    df_aa['ca_id'] = df_aa['ca_id'].astype(int)
    df_aa['ca_tax_st'] = df_aa['ca_tax_st'].astype(int)
    df_aa['ca_b_id'] = df_aa['ca_b_id'].astype(int)
    df_aa['ca_name'] = df_aa['ca_name'].astype(str)

    # Add missing columns with NULL values to match the first DataFrame schema
    missing_cols = [
        'c_dob', 'c_gndr', 'c_tax_id', 'c_tier', 'c_l_name', 'c_f_name', 'c_m_name', 
        'c_adline1', 'c_adline2', 'c_zipcode', 'c_city', 'c_state_prov', 'c_ctry', 
        'c_prim_email', 'c_alt_email', 'c_ctry_code_1', 'c_area_code_1', 'c_local_1', 
        'c_ctry_code_2', 'c_area_code_2', 'c_local_2', 'c_ctry_code_3', 'c_area_code_3', 
        'c_local_3', 'c_lcl_tx_id', 'c_nat_tx_id', 'card', 'c_phone_1_ctry_code', 
        'c_phone_1_area_code', 'c_phone_1_c_local', 'c_phone_2_ctry_code', 
        'c_phone_2_area_code', 'c_phone_2_c_local', 'c_phone_3_ctry_code', 
        'c_phone_3_area_code', 'c_phone_3_c_local'
    ]
    for col in missing_cols:
        df_aa[col] = None

    for col in df_aa.columns:
        df_aa[col] = df_aa[col].replace('nan', None)
    df_aa.replace('None', None, inplace=True)

    df_aa['action_type'] = 'AddAcct'
################################################################################
    missing_cols = [
        'action_ts', 'action_type', 'c_id', 'ca_id', 'ca_tax_st', 'ca_b_id', 'ca_name', 
        'c_tier', 'c_adline1', 'c_zipcode', 'c_city', 'c_state_prov', 'c_ctry', 
        'c_prim_email', 'c_phone_1_ctry_code', 'c_phone_1_area_code', 'c_phone_1_c_local', 
        'c_phone_2_ctry_code', 'c_phone_2_area_code', 'c_phone_2_c_local', 
        'c_phone_3_ctry_code', 'c_phone_3_area_code', 'c_phone_3_c_local', 'c_dob', 
        'c_gndr', 'c_tax_id', 'c_l_name', 'c_f_name', 'c_m_name', 'c_adline2', 
        'c_alt_email', 'c_ctry_code_1', 'c_area_code_1', 'c_local_1', 'c_ctry_code_2', 
        'c_area_code_2', 'c_local_2', 'c_ctry_code_3', 'c_area_code_3', 'c_local_3', 
        'c_lcl_tx_id', 'c_nat_tx_id', 'card'
    ]
    data_ic = client.get_object("lakehouse", "raw/crm/InactCust.csv")
    column_names_ic = ['action_ts', 'c_id']

    df_ic = pd.read_csv(BytesIO(data_ic.read()), header=None, names=column_names_ic)
    df_ic = df_ic.where(pd.notnull(df_ic), None)

    # Adjust data types
    df_ic['action_ts'] = pd.to_datetime(df_ic['action_ts'])
    df_ic['c_id'] = df_ic['c_id'].astype(int)

    # Add missing columns with NULL values to match the first DataFrame schema
    missing_cols_ic = [col for col in missing_cols if col not in column_names_ic]
    for col in missing_cols_ic:
        df_ic[col] = None

    df_ic['action_type'] = 'InactCust'

    # UpdAcct
    data_ua = client.get_object("lakehouse", "raw/crm/UpdAcct.csv")
    column_names_ua = ['action_ts', 'c_id', 'ca_id', 'ca_tax_st', 'ca_b_id', 'ca_name']

    df_ua = pd.read_csv(BytesIO(data_ua.read()), header=None, names=column_names_ua)
    df_ua = df_ua.where(pd.notnull(df_ua), None)

    # Adjust data types
    df_ua['action_ts'] = pd.to_datetime(df_ua['action_ts'])
    df_ua['c_id'] = df_ua['c_id'].astype(int)
    df_ua['ca_id'] = df_ua['ca_id'].astype(int)
    df_ua['ca_tax_st'] = pd.to_numeric(df_ua['ca_tax_st'], errors='coerce')
    df_ua['ca_tax_st'] = df_ua['ca_tax_st'].where(df_ua['ca_tax_st'].notnull(), None)
    df_ua['ca_b_id'] = pd.to_numeric(df_ua['ca_b_id'], errors='coerce')
    df_ua['ca_b_id'] = df_ua['ca_b_id'].where(df_ua['ca_b_id'].notnull(), None)
    df_ua['ca_name'] = df_ua['ca_name'].astype(str)

    # Add missing columns with NULL values to match the first DataFrame schema
    missing_cols_ua = [col for col in missing_cols if col not in column_names_ua]
    for col in missing_cols_ua:
        df_ua[col] = None

    df_ua['action_type'] = 'UpdAcct'

    # UpdCust
    data_uc = client.get_object("lakehouse", "raw/crm/UpdCust.csv")
    column_names_uc = [
        'action_ts', 'c_id', 'c_tier', 'c_adline1', 'c_zipcode', 'c_city', 
        'c_state_prov', 'c_ctry', 'c_prim_email', 'c_phone_1_ctry_code', 
        'c_phone_1_area_code', 'c_phone_1_c_local', 'c_phone_2_ctry_code', 
        'c_phone_2_area_code', 'c_phone_2_c_local', 'c_phone_3_ctry_code', 
        'c_phone_3_area_code', 'c_phone_3_c_local'
    ]

    df_uc = pd.read_csv(BytesIO(data_uc.read()), header=None, names=column_names_uc)
    df_uc = df_uc.where(pd.notnull(df_uc), None)

    # Adjust data types
    df_uc['action_ts'] = pd.to_datetime(df_uc['action_ts'])
    df_uc['c_id'] = df_uc['c_id'].astype(int)
    df_uc[column_names_uc[2:]] = df_uc[column_names_uc[2:]].astype(str)

    # Add missing columns with NULL values to match the first DataFrame schema
    missing_cols_uc = [col for col in missing_cols if col not in column_names_uc]
    for col in missing_cols_uc:
        df_uc[col] = None

    df_uc['action_type'] = 'UpdCust'

############################ Concatenate DataFrames ############################
    df = pd.concat([df_nc, df_aa, df_ic, df_ua, df_uc], ignore_index=True)
    create_query = """
        CREATE TABLE IF NOT EXISTS crm (
            action_ts timestamp(6),
            action_type VARCHAR,
            c_id INT,
            ca_id INT,
            ca_tax_st INT,
            ca_b_id INT,
            ca_name VARCHAR,
            c_tier VARCHAR,
            c_adline1 VARCHAR,
            c_zipcode VARCHAR,
            c_city VARCHAR,
            c_state_prov VARCHAR,
            c_ctry VARCHAR,
            c_prim_email VARCHAR,
            c_phone_1_ctry_code VARCHAR,
            c_phone_1_area_code VARCHAR,
            c_phone_1_c_local VARCHAR,
            c_phone_2_ctry_code VARCHAR,
            c_phone_2_area_code VARCHAR,
            c_phone_2_c_local VARCHAR,
            c_phone_3_ctry_code VARCHAR,
            c_phone_3_area_code VARCHAR,
            c_phone_3_c_local VARCHAR,
            c_dob DATE,
            c_gndr VARCHAR,
            c_tax_id VARCHAR,
            c_l_name VARCHAR,
            c_f_name VARCHAR,
            c_m_name VARCHAR,
            c_adline2 VARCHAR,
            c_alt_email VARCHAR,
            c_ctry_code_1 VARCHAR,
            c_area_code_1 VARCHAR,
            c_local_1 VARCHAR,
            c_ctry_code_2 VARCHAR,
            c_area_code_2 VARCHAR,
            c_local_2 VARCHAR,
            c_ctry_code_3 VARCHAR,
            c_area_code_3 VARCHAR,
            c_local_3 VARCHAR,
            c_lcl_tx_id VARCHAR,
            c_nat_tx_id VARCHAR,
            card VARCHAR
        ) WITH (format='PARQUET')
        """
    
    return df, create_query

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def crm():
    return read_csv_from_minio()

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def finwire_sec():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    objects = client.list_objects("lakehouse", prefix="raw/finwire/", recursive=True)

    sec_files = [obj.object_name for obj in objects if fnmatch.fnmatch(obj.object_name, '*_SEC.csv')]

    if sec_files:
        data_sec = client.get_object("lakehouse", sec_files[0])
    column_names_sec = ['pts', 'recType', 'issueType', 'status', 'name', 'exId', 'shOut', 'firstTradeDate', 'firstTradeExchg', 'dividend', 'coNameOrCIK']

    df_sec = pd.read_csv(BytesIO(data_sec.read()), header=None, names=column_names_sec)
    df_sec = df_sec.where(pd.notnull(df_sec), None)

    df_sec['pts'] = pd.to_datetime(df_sec['pts'])
    df_sec['recType'] = df_sec['recType'].astype(str)
    df_sec['issueType'] = df_sec['issueType'].astype(str)
    df_sec['status'] = df_sec['status'].astype(str)
    df_sec['name'] = df_sec['name'].astype(str)
    df_sec['exId'] = df_sec['exId'].astype(str)
    df_sec['shOut'] = df_sec['shOut'].astype('float32')  # Cast to float32 to match REAL in Trino
    df_sec['firstTradeDate'] = pd.to_datetime(df_sec['firstTradeDate'])
    df_sec['firstTradeExchg'] = df_sec['firstTradeExchg'].astype(str)
    df_sec['dividend'] = df_sec['dividend'].astype('float32')  # Cast to float32 to match REAL in Trino
    df_sec['coNameOrCIK'] = df_sec['coNameOrCIK'].astype(str)

    return df_sec, """
        CREATE TABLE IF NOT EXISTS finwire_sec (
            pts timestamp(6),
            recType VARCHAR,
            issueType VARCHAR,
            status VARCHAR,
            name VARCHAR,
            exId VARCHAR,
            shOut REAL,
            firstTradeDate timestamp(6),
            firstTradeExchg VARCHAR,
            dividend REAL,
            coNameOrCIK VARCHAR
        ) WITH (format='PARQUET')
    """
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def finwire_fin():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    objects = client.list_objects("lakehouse", prefix="raw/finwire/", recursive=True)

    # Filter the objects based on their names
    fin_files = [obj.object_name for obj in objects if fnmatch.fnmatch(obj.object_name, '*_FIN.csv')]

    # Now you have a list of all files that end with '_FIN.csv'
    # You can get each file with client.get_object("lakehouse", file_name)

    # Example for the first file
    if fin_files:
        data_fin = client.get_object("lakehouse", fin_files[0])
        column_names_fin = ['pts', 'recType', 'year', 'quarter', 'qtrStartDate', 'postingDate', 'revenue', 'earnings', 'eps', 'diluted_eps', 'margin', 'inventory', 'assets', 'liabilities', 'shOut', 'dilutedShOut', 'coNameOrCIK']

        df_fin = pd.read_csv(BytesIO(data_fin.read()), header=None, names=column_names_fin)
    df_fin = df_fin.where(pd.notnull(df_fin), None)

    # Adjust data types
    df_fin['pts'] = df_fin['pts'].astype(str)
    df_fin['recType'] = df_fin['recType'].astype(str)
    df_fin['year'] = df_fin['year'].astype(int)
    df_fin['quarter'] = df_fin['quarter'].astype(int)
    df_fin['qtrStartDate'] = pd.to_datetime(df_fin['qtrStartDate'])
    df_fin['postingDate'] = pd.to_datetime(df_fin['postingDate'])
    df_fin['revenue'] = df_fin['revenue'].astype('float32')
    df_fin['earnings'] = df_fin['earnings'].astype('float32')
    df_fin['eps'] = df_fin['eps'].astype('float32')
    df_fin['diluted_eps'] = df_fin['diluted_eps'].astype('float32')
    df_fin['margin'] = df_fin['margin'].astype('float32')
    df_fin['inventory'] = df_fin['inventory'].astype('float32')
    df_fin['assets'] = df_fin['assets'].astype('float32')
    df_fin['liabilities'] = df_fin['liabilities'].astype('float32')
    df_fin['shOut'] = df_fin['shOut'].astype('float32')
    df_fin['dilutedShOut'] = df_fin['dilutedShOut'].astype('float32')
    df_fin['coNameOrCIK'] = df_fin['coNameOrCIK'].astype(str)

    return df_fin, """
            CREATE TABLE IF NOT EXISTS finwire_fin (
                pts VARCHAR,
                recType VARCHAR,
                year INTEGER,
                quarter INTEGER,
                qtrStartDate DATE,
                postingDate DATE,
                revenue REAL,
                earnings REAL,
                eps REAL,
                diluted_eps REAL,
                margin REAL,
                inventory REAL,
                assets REAL,
                liabilities REAL,
                shOut REAL,
                dilutedShOut REAL,
                coNameOrCIK VARCHAR
            ) WITH (format='PARQUET')
        """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def finwire_cmp():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    objects = client.list_objects("lakehouse", prefix="raw/finwire/", recursive=True)

    # Filter the objects based on their names
    cmp_files = [obj.object_name for obj in objects if fnmatch.fnmatch(obj.object_name, '*_CMP.csv')]


    if cmp_files:
        data_cmp = client.get_object("lakehouse", cmp_files[0])
        column_names_cmp = ['pts', 'recType', 'companyName', 'cik', 'status', 'industryID', 'spRating', 'foundingDate', 'addr_line1', 'addr_line2', 'postalCode', 'city', 'stateProvince', 'country', 'ceoName', 'description']

        df_cmp = pd.read_csv(BytesIO(data_cmp.read()), header=None, names=column_names_cmp)
        df_cmp = df_cmp.where(pd.notnull(df_cmp), None)

        # Adjust data types
        df_cmp['pts'] = df_cmp['pts'].astype(str)
        df_cmp['recType'] = df_cmp['recType'].astype(str)
        df_cmp['companyName'] = df_cmp['companyName'].astype(str)
        df_cmp['cik'] = df_cmp['cik'].astype(str)
        df_cmp['status'] = df_cmp['status'].astype(str)
        df_cmp['industryID'] = df_cmp['industryID'].astype(str)
        df_cmp['spRating'] = df_cmp['spRating'].astype(str)
        df_cmp['foundingDate'] = pd.to_datetime(df_cmp['foundingDate']).dt.date  # Convert to date
        df_cmp['addr_line1'] = df_cmp['addr_line1'].astype(str)
        df_cmp['addr_line2'] = df_cmp['addr_line2'].astype(str)
        df_cmp['postalCode'] = df_cmp['postalCode'].astype(str)
        df_cmp['city'] = df_cmp['city'].astype(str)
        df_cmp['stateProvince'] = df_cmp['stateProvince'].astype(str)
        df_cmp['country'] = df_cmp['country'].astype(str)
        df_cmp['ceoName'] = df_cmp['ceoName'].astype(str)
        df_cmp['description'] = df_cmp['description'].astype(str)

        return df_cmp, """
            CREATE TABLE IF NOT EXISTS finwire_cmp (
                pts VARCHAR,
                recType VARCHAR,
                companyName VARCHAR,
                cik VARCHAR,
                status VARCHAR,
                industryID VARCHAR,
                spRating VARCHAR,
                foundingDate DATE,
                addr_line1 VARCHAR,
                addr_line2 VARCHAR,
                postalCode VARCHAR,
                city VARCHAR,
                stateProvince VARCHAR,
                country VARCHAR,
                ceoName VARCHAR,
                description VARCHAR
            ) WITH (format='PARQUET')
        """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def hr_employee():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_employee = client.get_object("lakehouse", "raw/hr/HR.csv")
    column_names_employee = ['employeeID', 'managerID', 'employeeFirstName', 'employeeLastName', 'employeeMI', 'employeeJobCode', 'employeeBranch', 'employeeOffice', 'employeePhone']

    df_employee = pd.read_csv(BytesIO(data_employee.read()), header=None, names=column_names_employee)
    df_employee = df_employee.where(pd.notnull(df_employee), None)

    # Adjust data types
    df_employee['employeeID'] = pd.to_numeric(df_employee['employeeID'], errors='coerce').astype('Int64')
    df_employee['managerID'] = pd.to_numeric(df_employee['managerID'], errors='coerce').astype('Int64')
    df_employee['employeeFirstName'] = df_employee['employeeFirstName'].astype(str)
    df_employee['employeeLastName'] = df_employee['employeeLastName'].astype(str)
    df_employee['employeeMI'] = df_employee['employeeMI'].astype(str)
    df_employee['employeeJobCode'] = pd.to_numeric(df_employee['employeeJobCode'], errors='coerce').astype('Int64')
    df_employee['employeeBranch'] = df_employee['employeeBranch'].astype(str)
    df_employee['employeeOffice'] = df_employee['employeeOffice'].astype(str)
    df_employee['employeePhone'] = df_employee['employeePhone'].astype(str)
    df_employee.replace('None', None, inplace=True)
    return df_employee, """
        CREATE TABLE IF NOT EXISTS hr_employee (
            employeeID INTEGER,
            managerID INTEGER,
            employeeFirstName VARCHAR,
            employeeLastName VARCHAR,
            employeeMI VARCHAR,
            employeeJobCode INTEGER,
            employeeBranch VARCHAR,
            employeeOffice VARCHAR,
            employeePhone VARCHAR
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_account():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    objects = client.list_objects('lakehouse', prefix='raw/oltp/Account', recursive=True)
    df_account = pd.DataFrame()
    for obj in objects:
        data_account = client.get_object('lakehouse', obj.object_name)
        column_names_account = ['cdc_flag', 'cdc_dsn', 'ca_id', 'ca_b_id', 'ca_c_id', 'ca_name', 'ca_tax_st', 'ca_st_id']
        df_tmp = pd.read_csv(BytesIO(data_account.read()), sep='|', header=None, names=column_names_account)
        df_account = pd.concat([df_account, df_tmp], ignore_index=True)
    df_account = df_account.where(pd.notnull(df_account), None)

    # Adjust data types
    df_account['cdc_flag'] = df_account['cdc_flag'].astype(str)
    df_account['cdc_dsn'] = pd.to_numeric(df_account['cdc_dsn'], errors='coerce').astype('Int64')
    df_account['ca_id'] = pd.to_numeric(df_account['ca_id'], errors='coerce').astype('Int64')
    df_account['ca_b_id'] = pd.to_numeric(df_account['ca_b_id'], errors='coerce').astype('Int64')
    df_account['ca_c_id'] = pd.to_numeric(df_account['ca_c_id'], errors='coerce').astype('Int64')
    df_account['ca_name'] = df_account['ca_name'].astype(str)
    df_account['ca_tax_st'] = pd.to_numeric(df_account['ca_tax_st'], errors='coerce').astype('Int64')
    df_account['ca_st_id'] = df_account['ca_st_id'].astype(str)
    df_account.replace('None', None, inplace=True)
    return df_account, """
        CREATE TABLE IF NOT EXISTS oltp_account (
            cdc_flag VARCHAR,
            cdc_dsn INTEGER,
            ca_id INTEGER,
            ca_b_id INTEGER,
            ca_c_id INTEGER,
            ca_name VARCHAR,
            ca_tax_st INTEGER,
            ca_st_id VARCHAR
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_cashtransactions():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_cashtransactionhistorical = client.get_object("lakehouse", "raw/oltp/CashTransactionHistorical.txt")
    column_names_cashtransactionhistorical = ['ct_ca_id', 'ct_dts', 'ct_amt', 'ct_name']

    df_cashtransactionhistorical = pd.read_csv(BytesIO(data_cashtransactionhistorical.read()), sep='|', header=None, names=column_names_cashtransactionhistorical)
    df_cashtransactionhistorical = df_cashtransactionhistorical.where(pd.notnull(df_cashtransactionhistorical), None)

    # Adjust data types
    df_cashtransactionhistorical['ct_ca_id'] = pd.to_numeric(df_cashtransactionhistorical['ct_ca_id'], errors='coerce').astype('Int64')
    df_cashtransactionhistorical['ct_dts'] = pd.to_datetime(df_cashtransactionhistorical['ct_dts'], errors='coerce')
    df_cashtransactionhistorical['ct_amt'] = pd.to_numeric(df_cashtransactionhistorical['ct_amt'], errors='coerce').astype('float64')
    df_cashtransactionhistorical['ct_name'] = df_cashtransactionhistorical['ct_name'].astype(str)
    df_cashtransactionhistorical.replace('None', None, inplace=True)
    return df_cashtransactionhistorical, """
        CREATE TABLE IF NOT EXISTS oltp_cashtransactions (
            ct_ca_id INTEGER,
            ct_dts TIMESTAMP(6),
            ct_amt REAL,
            ct_name VARCHAR
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_dailymarket():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_dailymarket = client.get_object("lakehouse", "raw/oltp/DailyMarketHistorical.txt")
    column_names_dailymarket = ['dm_date', 'dm_s_symb', 'dm_close', 'dm_high', 'dm_low', 'dm_vol']

    df_dailymarket = pd.read_csv(BytesIO(data_dailymarket.read()), sep='|', header=None, names=column_names_dailymarket)
    df_dailymarket = df_dailymarket.where(pd.notnull(df_dailymarket), None)

    # Adjust data types
    df_dailymarket['dm_date'] = pd.to_datetime(df_dailymarket['dm_date'], errors='coerce').dt.date
    df_dailymarket['dm_s_symb'] = df_dailymarket['dm_s_symb'].astype(str)
    df_dailymarket['dm_close'] = pd.to_numeric(df_dailymarket['dm_close'], errors='coerce').astype('float64')
    df_dailymarket['dm_high'] = pd.to_numeric(df_dailymarket['dm_high'], errors='coerce').astype('float64')
    df_dailymarket['dm_low'] = pd.to_numeric(df_dailymarket['dm_low'], errors='coerce').astype('float64')
    df_dailymarket['dm_vol'] = pd.to_numeric(df_dailymarket['dm_vol'], errors='coerce').astype('Int64')
    df_dailymarket.replace('None', None, inplace=True)
    return df_dailymarket, """
        CREATE TABLE IF NOT EXISTS oltp_dailymarket (
            dm_date DATE,
            dm_s_symb VARCHAR,
            dm_close REAL,
            dm_high REAL,
            dm_low REAL,
            dm_vol INTEGER
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_holdinghistory():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_holdinghistory = client.get_object("lakehouse", "raw/oltp/HoldingHistoryHistorical.txt")
    column_names_holdinghistory = ['hh_h_t_id', 'hh_t_id', 'hh_before_qty', 'hh_after_qty']

    df_holdinghistory = pd.read_csv(BytesIO(data_holdinghistory.read()), sep='|', header=None, names=column_names_holdinghistory)
    df_holdinghistory = df_holdinghistory.where(pd.notnull(df_holdinghistory), None)

    # Adjust data types
    df_holdinghistory['hh_h_t_id'] = pd.to_numeric(df_holdinghistory['hh_h_t_id'], errors='coerce').astype('Int64')
    df_holdinghistory['hh_t_id'] = pd.to_numeric(df_holdinghistory['hh_t_id'], errors='coerce').astype('Int64')
    df_holdinghistory['hh_before_qty'] = pd.to_numeric(df_holdinghistory['hh_before_qty'], errors='coerce').astype('Int64')
    df_holdinghistory['hh_after_qty'] = pd.to_numeric(df_holdinghistory['hh_after_qty'], errors='coerce').astype('Int64')
    df_holdinghistory.replace('None', None, inplace=True)
    return df_holdinghistory, """
        CREATE TABLE IF NOT EXISTS oltp_holdinghistory (
            hh_h_t_id INTEGER,
            hh_t_id INTEGER,
            hh_before_qty INTEGER,
            hh_after_qty INTEGER
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_tradehistory():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_tradehistory = client.get_object("lakehouse", "raw/oltp/TradeHistory.txt")
    column_names_tradehistory = ['th_t_id', 'th_dts', 'th_st_id']

    df_tradehistory = pd.read_csv(BytesIO(data_tradehistory.read()), sep='|', header=None, names=column_names_tradehistory)
    df_tradehistory = df_tradehistory.where(pd.notnull(df_tradehistory), None)

    # Adjust data types
    df_tradehistory['th_t_id'] = pd.to_numeric(df_tradehistory['th_t_id'], errors='coerce').astype('Int64')
    df_tradehistory['th_dts'] = pd.to_datetime(df_tradehistory['th_dts'], errors='coerce')
    df_tradehistory['th_st_id'] = df_tradehistory['th_st_id'].astype(str)
    df_tradehistory.replace('None', None, inplace=True)
    return df_tradehistory, """
        CREATE TABLE IF NOT EXISTS oltp_tradehistory (
            th_t_id INTEGER,
            th_dts TIMESTAMP(6),
            th_st_id VARCHAR
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_trade():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_trade = client.get_object("lakehouse", "raw/oltp/TradeHistorical.txt")
    column_names_trade = ['t_id', 't_dts', 't_st_id', 't_tt_id', 't_is_cash', 't_s_symb', 't_qty', 't_bid_price', 't_ca_id', 't_exec_name', 't_trade_price', 't_chrg', 't_comm', 't_tax']

    df_trade = pd.read_csv(BytesIO(data_trade.read()), sep='|', header=None, names=column_names_trade)
    df_trade = df_trade.where(pd.notnull(df_trade), None)

    # Adjust data types
    df_trade['t_id'] = pd.to_numeric(df_trade['t_id'], errors='coerce').astype('Int64')
    df_trade['t_dts'] = pd.to_datetime(df_trade['t_dts'], errors='coerce')
    df_trade['t_st_id'] = df_trade['t_st_id'].astype(str)
    df_trade['t_tt_id'] = df_trade['t_tt_id'].astype(str)
    df_trade['t_is_cash'] = df_trade['t_is_cash'].astype(bool)
    df_trade['t_s_symb'] = df_trade['t_s_symb'].astype(str)
    df_trade['t_qty'] = pd.to_numeric(df_trade['t_qty'], errors='coerce').astype('Int64')
    df_trade['t_bid_price'] = pd.to_numeric(df_trade['t_bid_price'], errors='coerce').astype('float64')
    df_trade['t_ca_id'] = pd.to_numeric(df_trade['t_ca_id'], errors='coerce').astype('Int64')
    df_trade['t_exec_name'] = df_trade['t_exec_name'].astype(str)
    df_trade['t_trade_price'] = pd.to_numeric(df_trade['t_trade_price'], errors='coerce').astype('float64')
    df_trade['t_chrg'] = pd.to_numeric(df_trade['t_chrg'], errors='coerce').astype('float64')
    df_trade['t_comm'] = pd.to_numeric(df_trade['t_comm'], errors='coerce').astype('float64')
    df_trade['t_tax'] = pd.to_numeric(df_trade['t_tax'], errors='coerce').astype('float64')
    df_trade.replace('None', None, inplace=True)
    return df_trade, """
        CREATE TABLE IF NOT EXISTS oltp_trade (
            t_id INTEGER,
            t_dts TIMESTAMP(6),
            t_st_id VARCHAR,
            t_tt_id VARCHAR,
            t_is_cash BOOLEAN,
            t_s_symb VARCHAR,
            t_qty INTEGER,
            t_bid_price REAL,
            t_ca_id INTEGER,
            t_exec_name VARCHAR,
            t_trade_price REAL,
            t_chrg REAL,
            t_comm REAL,
            t_tax REAL
        ) WITH (format='PARQUET')
    """

      
@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def oltp_watchhistory():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_watchhistoryhistorical = client.get_object("lakehouse", "raw/oltp/WatchHistoryHistorical.txt")
    column_names_watchhistoryhistorical = ['w_c_id', 'w_s_symb', 'w_dts', 'w_action']

    df_watchhistoryhistorical = pd.read_csv(BytesIO(data_watchhistoryhistorical.read()), sep='|', header=None, names=column_names_watchhistoryhistorical)
    df_watchhistoryhistorical = df_watchhistoryhistorical.where(pd.notnull(df_watchhistoryhistorical), None)

    # Adjust data types
    df_watchhistoryhistorical['w_c_id'] = pd.to_numeric(df_watchhistoryhistorical['w_c_id'], errors='coerce').astype('Int64')
    df_watchhistoryhistorical['w_s_symb'] = df_watchhistoryhistorical['w_s_symb'].astype(str)
    df_watchhistoryhistorical['w_dts'] = pd.to_datetime(df_watchhistoryhistorical['w_dts'], errors='coerce')
    df_watchhistoryhistorical['w_action'] = df_watchhistoryhistorical['w_action'].astype(str)
    df_watchhistoryhistorical.replace('None', None, inplace=True)
    return df_watchhistoryhistorical, """
        CREATE TABLE IF NOT EXISTS oltp_watchhistory (
            w_c_id INTEGER,
            w_s_symb VARCHAR,
            w_dts TIMESTAMP(6),
            w_action VARCHAR
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def reference_date():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_date = client.get_object("lakehouse", "raw/reference/Date.txt")
    column_names_date = ['SK_DateID', 'DateValue', 'DateDesc', 'CalendarYearID', 'CalendarYearDesc', 'CalendarQtrID', 'CalendarQtrDesc', 'CalendarMonthID', 'CalendarMonthDesc', 'CalendarWeekID', 'CalendarWeekDesc', 'DayOfWeekNum', 'DayOfWeekDesc', 'FiscalYearID', 'FiscalYearDesc', 'FiscalQtrID', 'FiscalQtrDesc', 'HolidayFlag']

    df_date = pd.read_csv(BytesIO(data_date.read()), sep='|', header=None, names=column_names_date)
    df_date = df_date.where(pd.notnull(df_date), None)

    # Adjust data types
    df_date['SK_DateID'] = pd.to_numeric(df_date['SK_DateID'], errors='coerce').astype('Int64')
    df_date['DateValue'] = df_date['DateValue'].astype(str)
    df_date['DateDesc'] = df_date['DateDesc'].astype(str)
    df_date['CalendarYearID'] = pd.to_numeric(df_date['CalendarYearID'], errors='coerce').astype('Int64')
    df_date['CalendarYearDesc'] = df_date['CalendarYearDesc'].astype(str)
    df_date['CalendarQtrID'] = pd.to_numeric(df_date['CalendarQtrID'], errors='coerce').astype('Int64')
    df_date['CalendarQtrDesc'] = df_date['CalendarQtrDesc'].astype(str)
    df_date['CalendarMonthID'] = pd.to_numeric(df_date['CalendarMonthID'], errors='coerce').astype('Int64')
    df_date['CalendarMonthDesc'] = df_date['CalendarMonthDesc'].astype(str)
    df_date['CalendarWeekID'] = pd.to_numeric(df_date['CalendarWeekID'], errors='coerce').astype('Int64')
    df_date['CalendarWeekDesc'] = df_date['CalendarWeekDesc'].astype(str)
    df_date['DayOfWeekNum'] = pd.to_numeric(df_date['DayOfWeekNum'], errors='coerce').astype('Int64')
    df_date['DayOfWeekDesc'] = df_date['DayOfWeekDesc'].astype(str)
    df_date['FiscalYearID'] = pd.to_numeric(df_date['FiscalYearID'], errors='coerce').astype('Int64')
    df_date['FiscalYearDesc'] = df_date['FiscalYearDesc'].astype(str)
    df_date['FiscalQtrID'] = pd.to_numeric(df_date['FiscalQtrID'], errors='coerce').astype('Int64')
    df_date['FiscalQtrDesc'] = df_date['FiscalQtrDesc'].astype(str)
    df_date['HolidayFlag'] = df_date['HolidayFlag'].astype(bool)
    df_date.replace('None', None, inplace=True)
    return df_date, """
        CREATE TABLE IF NOT EXISTS reference_date (
            SK_DateID INTEGER,
            DateValue VARCHAR,
            DateDesc VARCHAR,
            CalendarYearID INTEGER,
            CalendarYearDesc VARCHAR,
            CalendarQtrID INTEGER,
            CalendarQtrDesc VARCHAR,
            CalendarMonthID INTEGER,
            CalendarMonthDesc VARCHAR,
            CalendarWeekID INTEGER,
            CalendarWeekDesc VARCHAR,
            DayOfWeekNum INTEGER,
            DayOfWeekDesc VARCHAR,
            FiscalYearID INTEGER,
            FiscalYearDesc VARCHAR,
            FiscalQtrID INTEGER,
            FiscalQtrDesc VARCHAR,
            HolidayFlag BOOLEAN
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def reference_industry():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_industry = client.get_object("lakehouse", "raw/reference/Industry.txt")
    column_names_industry = ['in_id', 'in_name', 'in_sc_id']

    df_industry = pd.read_csv(BytesIO(data_industry.read()), sep='|', header=None, names=column_names_industry)
    df_industry = df_industry.where(pd.notnull(df_industry), None)

    # Adjust data types
    df_industry['in_id'] = df_industry['in_id'].astype(str)
    df_industry['in_name'] = df_industry['in_name'].astype(str)
    df_industry['in_sc_id'] = df_industry['in_sc_id'].astype(str)
    df_industry.replace('None', None, inplace=True)
    return df_industry, """
        CREATE TABLE IF NOT EXISTS reference_industry (
            in_id VARCHAR,
            in_name VARCHAR,
            in_sc_id VARCHAR
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def reference_statustype():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_statustype = client.get_object("lakehouse", "raw/reference/StatusType.txt")
    column_names_statustype = ['st_id', 'st_name']

    df_statustype = pd.read_csv(BytesIO(data_statustype.read()), sep='|', header=None, names=column_names_statustype)
    df_statustype = df_statustype.where(pd.notnull(df_statustype), None)

    # Adjust data types
    df_statustype['st_id'] = df_statustype['st_id'].astype(str)
    df_statustype['st_name'] = df_statustype['st_name'].astype(str)
    df_statustype.replace('None', None, inplace=True)
    return df_statustype, """
        CREATE TABLE IF NOT EXISTS reference_statustype (
            st_id VARCHAR,
            st_name VARCHAR
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def reference_taxrate():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_taxrate = client.get_object("lakehouse", "raw/reference/TaxRate.txt")
    column_names_taxrate = ['tx_id', 'tx_name', 'tx_rate']

    df_taxrate = pd.read_csv(BytesIO(data_taxrate.read()), sep='|', header=None, names=column_names_taxrate)
    df_taxrate = df_taxrate.where(pd.notnull(df_taxrate), None)

    # Adjust data types
    df_taxrate['tx_id'] = df_taxrate['tx_id'].astype(str)
    df_taxrate['tx_name'] = df_taxrate['tx_name'].astype(str)
    df_taxrate['tx_rate'] = df_taxrate['tx_rate'].astype(str)
    df_taxrate.replace('None', None, inplace=True)
    return df_taxrate, """
        CREATE TABLE IF NOT EXISTS reference_taxrate (
            tx_id VARCHAR,
            tx_name VARCHAR,
            tx_rate VARCHAR
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def reference_time():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    data_time = client.get_object("lakehouse", "raw/reference/Time.txt")
    column_names_time = ['SK_TimeID', 'TimeValue', 'HourID', 'HourDesc', 'MinuteID', 'MinuteDesc', 'SecondID', 'SecondDesc', 'MarketHoursFlag', 'OfficeHoursFlag']

    df_time = pd.read_csv(BytesIO(data_time.read()), sep='|', header=None, names=column_names_time)
    df_time = df_time.where(pd.notnull(df_time), None)

    # Adjust data types
    df_time['SK_TimeID'] = pd.to_numeric(df_time['SK_TimeID'], errors='coerce').astype('Int64')
    df_time['TimeValue'] = df_time['TimeValue'].astype(str)
    df_time['HourID'] = pd.to_numeric(df_time['HourID'], errors='coerce').astype('Int64')
    df_time['HourDesc'] = df_time['HourDesc'].astype(str)
    df_time['MinuteID'] = pd.to_numeric(df_time['MinuteID'], errors='coerce').astype('Int64')
    df_time['MinuteDesc'] = df_time['MinuteDesc'].astype(str)
    df_time['SecondID'] = pd.to_numeric(df_time['SecondID'], errors='coerce').astype('Int64')
    df_time['SecondDesc'] = df_time['SecondDesc'].astype(str)
    df_time['MarketHoursFlag'] = df_time['MarketHoursFlag'].astype(bool)
    df_time['OfficeHoursFlag'] = df_time['OfficeHoursFlag'].astype(bool)
    df_time.replace('None', None, inplace=True)
    return df_time, """
        CREATE TABLE IF NOT EXISTS reference_time (
            SK_TimeID INTEGER,
            TimeValue VARCHAR,
            HourID INTEGER,
            HourDesc VARCHAR,
            MinuteID INTEGER,
            MinuteDesc VARCHAR,
            SecondID INTEGER,
            SecondDesc VARCHAR,
            MarketHoursFlag BOOLEAN,
            OfficeHoursFlag BOOLEAN
        ) WITH (format='PARQUET')
    """

@asset(io_manager_key="minio_iceberg_io_manager", compute_kind="Python", group_name="bronze")
def sales_prospect():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    objects = client.list_objects('lakehouse', prefix='raw/sales/Prospect', recursive=True)
    df_prospect = pd.DataFrame()
    for obj in objects:
        data_prospect = client.get_object('lakehouse', obj.object_name)
        column_names_prospect = ['agencyID', 'lastName', 'firstName', 'middleInitial', 'gender', 'addressLine1', 'addressLine2', 'postalCode', 'city', 'state', 'country', 'phone', 'income', 'numberCars', 'numberChildren', 'maritalStatus', 'age', 'creditRating', 'ownOrRentFlag', 'employer', 'numberCreditCards', 'netWorth']
        df_tmp = pd.read_csv(BytesIO(data_prospect.read()), header=None, names=column_names_prospect)
        df_prospect = pd.concat([df_prospect, df_tmp], ignore_index=True)
    df_prospect = df_prospect.where(pd.notnull(df_prospect), None)

    # Adjust data types
    df_prospect['agencyID'] = df_prospect['agencyID'].astype(str)
    df_prospect['lastName'] = df_prospect['lastName'].astype(str)
    df_prospect['firstName'] = df_prospect['firstName'].astype(str)
    df_prospect['middleInitial'] = df_prospect['middleInitial'].astype(str)
    df_prospect['gender'] = df_prospect['gender'].astype(str)
    df_prospect['addressLine1'] = df_prospect['addressLine1'].astype(str)
    df_prospect['addressLine2'] = df_prospect['addressLine2'].astype(str)
    df_prospect['postalCode'] = df_prospect['postalCode'].astype(str)
    df_prospect['city'] = df_prospect['city'].astype(str)
    df_prospect['state'] = df_prospect['state'].astype(str)
    df_prospect['country'] = df_prospect['country'].astype(str)
    df_prospect['phone'] = df_prospect['phone'].astype(str)
    df_prospect['income'] = pd.to_numeric(df_prospect['income'], errors='coerce').astype('Int64')
    df_prospect['numberCars'] = pd.to_numeric(df_prospect['numberCars'], errors='coerce').astype('Int64')
    df_prospect['numberChildren'] = pd.to_numeric(df_prospect['numberChildren'], errors='coerce').astype('Int64')
    df_prospect['maritalStatus'] = df_prospect['maritalStatus'].astype(str)
    df_prospect['age'] = pd.to_numeric(df_prospect['age'], errors='coerce').astype('Int64')
    df_prospect['creditRating'] = pd.to_numeric(df_prospect['creditRating'], errors='coerce').astype('Int64')
    df_prospect['ownOrRentFlag'] = df_prospect['ownOrRentFlag'].astype(str)
    df_prospect['employer'] = df_prospect['employer'].astype(str)
    df_prospect['numberCreditCards'] = pd.to_numeric(df_prospect['numberCreditCards'], errors='coerce').astype('Int64')
    df_prospect['netWorth'] = pd.to_numeric(df_prospect['netWorth'], errors='coerce').astype('Int64')
    df_prospect.replace('None', None, inplace=True)
    return df_prospect, """
        CREATE TABLE IF NOT EXISTS sales_prospect (
            agencyID VARCHAR,
            lastName VARCHAR,
            firstName VARCHAR,
            middleInitial VARCHAR,
            gender VARCHAR,
            addressLine1 VARCHAR,
            addressLine2 VARCHAR,
            postalCode VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            phone VARCHAR,
            income INTEGER,
            numberCars INTEGER,
            numberChildren INTEGER,
            maritalStatus VARCHAR,
            age INTEGER,
            creditRating INTEGER,
            ownOrRentFlag VARCHAR,
            employer VARCHAR,
            numberCreditCards INTEGER,
            netWorth INTEGER
        ) WITH (format='PARQUET')
    """
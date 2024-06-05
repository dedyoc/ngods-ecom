from dagster import asset

@asset(io_manager_key="trino_transform_io_manager", compute_kind="Iceberg", group_name="silver")
def transformed_employee(hr_employee):
    # Define your select statement
    select_query = """
        SELECT 
            employeeID as employee_id,
            managerID as manager_id,
            employeeFirstName AS first_name,
            employeeLastName AS last_name,
            employeeMI AS middle_initial,
            employeeJobCode AS job_code,
            employeeBranch AS branch,
            employeeOffice AS office,
            employeePhone AS phone
        FROM hr_employee
    """.strip() 
    return select_query, None

@asset(io_manager_key="trino_transform_io_manager", compute_kind="Iceberg", group_name="silver")
def accounts(crm):
    # Define your select statement
    select_query = """
        SELECT
            action_type,
            decode(action_type,
                'NEW', 'Active',
                'ADDACCT', 'Active',
                'UPDACCT', 'Active',
                'CLOSEACCT', 'Inactive') status,
            ca_id account_id,
            ca_name account_desc,
            c_id customer_id,
            c_tax_id tax_id,
            c_gndr gender,
            c_tier tier,
            c_dob dob,
            c_l_name last_name,
            c_f_name first_name,
            c_m_name middle_name,
            c_adline1 address_line1,
            c_adline2 address_line2,
            c_zipcode postal_code,
            c_city city,
            c_state_prov state_province,
            c_ctry country,
            c_prim_email primary_email,
            c_alt_email alternate_email,
            c_phone_1 phone1,
            c_phone_2 phone2,
            c_phone_3 phone3,
            c_lcl_tx_id local_tax_rate_name,
            ltx.tx_rate local_tax_rate,
            c_nat_tx_id national_tax_rate_name,
            ntx.tx_rate national_tax_rate,
            ca_tax_st tax_status,
            ca_b_id broker_id,
            action_ts AS effective_timestamp,
            -- Use DATE_ADD and TIMESTAMP
            ifnull(
                DATE_ADD(
                    'millisecond',
                    -1,
                    lag(action_ts) OVER (PARTITION BY ca_id ORDER BY action_ts DESC)
                ),
                TIMESTAMP '9999-12-31 23:59:59.999'
            ) AS end_timestamp,
            CASE
                WHEN (
                    row_number() OVER (PARTITION BY ca_id ORDER BY action_ts DESC) = 1
                ) THEN TRUE
                ELSE FALSE
            END AS IS_CURRENT
        FROM
            crm c
        LEFT JOIN
            reference_tax_rate ntx ON c.c_nat_tx_id = ntx.tx_id
        LEFT JOIN
            reference_tax_rate ltx ON c.c_lcl_tx_id = ltx.tx_id
        WHERE ca_id IS NOT NULL;
    """.strip() 
    return select_query, None
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    state_code INTEGER,
    district_code INTEGER,
    subdistrict_code INTEGER,
    town_village_code INTEGER,
    ward_code INTEGER,
    eb_code INTEGER,
    level VARCHAR(50),
    name TEXT,
    tru VARCHAR(10)  -- Total/Rural/Urban
);
CREATE TABLE households_and_population (
    location_id INTEGER REFERENCES locations(id),
    no_hh INTEGER,
    tot_p INTEGER,
    tot_m INTEGER,
    tot_f INTEGER,
    p_06 INTEGER,
    m_06 INTEGER,
    f_06 INTEGER,
    PRIMARY KEY (location_id)
);
CREATE TABLE scheduled_caste_tribe (
    location_id INTEGER REFERENCES locations(id),
    p_sc INTEGER,
    m_sc INTEGER, 
    f_sc INTEGER,
    p_st INTEGER, 
    m_st INTEGER, 
    f_st INTEGER,
    PRIMARY KEY (location_id)
);
CREATE TABLE literacy (
    location_id INTEGER REFERENCES locations(id),
    p_lit INTEGER, 
    m_lit INTEGER, 
    f_lit INTEGER,
    p_ill INTEGER, 
    m_ill INTEGER, 
    f_ill INTEGER,
    PRIMARY KEY (location_id)
);
CREATE TABLE workers_total (
    location_id INTEGER REFERENCES locations(id),
    tot_work_p INTEGER, tot_work_m INTEGER, tot_work_f INTEGER,
    non_work_p INTEGER, non_work_m INTEGER, non_work_f INTEGER,
    PRIMARY KEY (location_id)
);
CREATE TABLE main_workers (
    location_id INTEGER REFERENCES locations(id),
    mainwork_p INTEGER, mainwork_m INTEGER, mainwork_f INTEGER,
    main_cl_p INTEGER, main_cl_m INTEGER, main_cl_f INTEGER,
    main_al_p INTEGER, main_al_m INTEGER, main_al_f INTEGER,
    main_hh_p INTEGER, main_hh_m INTEGER, main_hh_f INTEGER,
    main_ot_p INTEGER, main_ot_m INTEGER, main_ot_f INTEGER,
    PRIMARY KEY (location_id)
);
CREATE TABLE marginal_workers (
    location_id INTEGER REFERENCES locations(id),
    margwork_p INTEGER, margwork_m INTEGER, margwork_f INTEGER,
    marg_cl_p INTEGER, marg_cl_m INTEGER, marg_cl_f INTEGER,
    marg_al_p INTEGER, marg_al_m INTEGER, marg_al_f INTEGER,
    marg_hh_p INTEGER, marg_hh_m INTEGER, marg_hh_f INTEGER,
    marg_ot_p INTEGER, marg_ot_m INTEGER, marg_ot_f INTEGER,
    margwork_3_6_p INTEGER, margwork_3_6_m INTEGER, margwork_3_6_f INTEGER,
    marg_cl_3_6_p INTEGER, marg_cl_3_6_m INTEGER, marg_cl_3_6_f INTEGER,
    marg_al_3_6_p INTEGER, marg_al_3_6_m INTEGER, marg_al_3_6_f INTEGER,
    marg_hh_3_6_p INTEGER, marg_hh_3_6_m INTEGER, marg_hh_3_6_f INTEGER,
    marg_ot_3_6_p INTEGER, marg_ot_3_6_m INTEGER, marg_ot_3_6_f INTEGER,
    margwork_0_3_p INTEGER, margwork_0_3_m INTEGER, margwork_0_3_f INTEGER,
    marg_cl_0_3_p INTEGER, marg_cl_0_3_m INTEGER, marg_cl_0_3_f INTEGER,
    marg_al_0_3_p INTEGER, marg_al_0_3_m INTEGER, marg_al_0_3_f INTEGER,
    marg_hh_0_3_p INTEGER, marg_hh_0_3_m INTEGER, marg_hh_0_3_f INTEGER,
    marg_ot_0_3_p INTEGER, marg_ot_0_3_m INTEGER, marg_ot_0_3_f INTEGER,
    PRIMARY KEY (location_id)
);

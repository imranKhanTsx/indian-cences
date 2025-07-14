async def insert_location_row(conn, row: dict, row_id: int):
    await conn.execute(
        """
        INSERT INTO locations (
            id, state_code, district_code, subdistrict_code,
            town_village_code, ward_code, eb_code,
            level, name, tru
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        row_id,
        int(row["State"]),
        int(row["District"]),
        int(row["Subdistt"]),
        int(row["Town/Village"]),
        int(row["Ward"]),
        int(row["EB"]),
        str(row["Level"]),
        str(row["Name"]),
        str(row["TRU"]),
    )
    print(f"✅ Inserted location with ID {row_id}")


async def insert_households_population_row(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO households_and_population (
            location_id, no_hh, tot_p, tot_m, tot_f,
            p_06, m_06, f_06
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        location_id,
        int(row["No_HH"]),
        int(row["TOT_P"]),
        int(row["TOT_M"]),
        int(row["TOT_F"]),
        int(row["P_06"]),
        int(row["M_06"]),
        int(row["F_06"]),
    )
    print(f"✅ Inserted households & population for location_id {location_id}")


async def insert_scheduled_caste_tribe(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO scheduled_caste_tribe (
            location_id, 
            p_sc, m_sc, f_sc, 
            p_st, m_st, f_st
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        location_id,
        int(row["P_SC"]),
        int(row["M_SC"]),
        int(row["F_SC"]),
        int(row["P_ST"]),
        int(row["M_ST"]),
        int(row["F_ST"]),
    )
    print(f"✅ Inserted caste/tribe data for location ID {location_id}")


async def insert_literacy_row(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO literacy (
            location_id,
            p_lit, m_lit, f_lit,
            p_ill, m_ill, f_ill
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        location_id,
        int(row["P_LIT"]),
        int(row["M_LIT"]),
        int(row["F_LIT"]),
        int(row["P_ILL"]),
        int(row["M_ILL"]),
        int(row["F_ILL"]),
    )
    print(f"✅ Inserted literacy data for location_id {location_id}")


async def insert_workers_total_row(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO workers_total (
            location_id,
            tot_work_p, tot_work_m, tot_work_f,
            non_work_p, non_work_m, non_work_f
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        location_id,
        int(row["TOT_WORK_P"]),
        int(row["TOT_WORK_M"]),
        int(row["TOT_WORK_F"]),
        int(row["NON_WORK_P"]),
        int(row["NON_WORK_M"]),
        int(row["NON_WORK_F"]),
    )
    print(f"✅ Inserted workers_total data for location_id {location_id}")


async def insert_main_workers_row(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO main_workers (
            location_id,
            mainwork_p, mainwork_m, mainwork_f,
            main_cl_p, main_cl_m, main_cl_f,
            main_al_p, main_al_m, main_al_f,
            main_hh_p, main_hh_m, main_hh_f,
            main_ot_p, main_ot_m, main_ot_f
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12, $13,
            $14, $15, $16
        )
        """,
        location_id,
        int(row["MAINWORK_P"]),
        int(row["MAINWORK_M"]),
        int(row["MAINWORK_F"]),
        int(row["MAIN_CL_P"]),
        int(row["MAIN_CL_M"]),
        int(row["MAIN_CL_F"]),
        int(row["MAIN_AL_P"]),
        int(row["MAIN_AL_M"]),
        int(row["MAIN_AL_F"]),
        int(row["MAIN_HH_P"]),
        int(row["MAIN_HH_M"]),
        int(row["MAIN_HH_F"]),
        int(row["MAIN_OT_P"]),
        int(row["MAIN_OT_M"]),
        int(row["MAIN_OT_F"]),
    )
    print(f"✅ Inserted main_workers data for location_id {location_id}")


async def insert_marginal_workers_row(conn, row: dict, location_id: int):
    await conn.execute(
        """
        INSERT INTO marginal_workers (
            location_id,
            margwork_p, margwork_m, margwork_f,
            marg_cl_p, marg_cl_m, marg_cl_f,
            marg_al_p, marg_al_m, marg_al_f,
            marg_hh_p, marg_hh_m, marg_hh_f,
            marg_ot_p, marg_ot_m, marg_ot_f,
            margwork_3_6_p, margwork_3_6_m, margwork_3_6_f,
            marg_cl_3_6_p, marg_cl_3_6_m, marg_cl_3_6_f,
            marg_al_3_6_p, marg_al_3_6_m, marg_al_3_6_f,
            marg_hh_3_6_p, marg_hh_3_6_m, marg_hh_3_6_f,
            marg_ot_3_6_p, marg_ot_3_6_m, marg_ot_3_6_f,
            margwork_0_3_p, margwork_0_3_m, margwork_0_3_f,
            marg_cl_0_3_p, marg_cl_0_3_m, marg_cl_0_3_f,
            marg_al_0_3_p, marg_al_0_3_m, marg_al_0_3_f,
            marg_hh_0_3_p, marg_hh_0_3_m, marg_hh_0_3_f,
            marg_ot_0_3_p, marg_ot_0_3_m, marg_ot_0_3_f
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12, $13,
            $14, $15, $16,
            $17, $18, $19,
            $20, $21, $22,
            $23, $24, $25,
            $26, $27, $28,
            $29, $30, $31,
            $32, $33, $34,
            $35, $36, $37,
            $38, $39, $40,
            $41, $42, $43,
            $44, $45, $46
        )
        """,
        location_id,
        int(row["MARGWORK_P"]),
        int(row["MARGWORK_M"]),
        int(row["MARGWORK_F"]),
        int(row["MARG_CL_P"]),
        int(row["MARG_CL_M"]),
        int(row["MARG_CL_F"]),
        int(row["MARG_AL_P"]),
        int(row["MARG_AL_M"]),
        int(row["MARG_AL_F"]),
        int(row["MARG_HH_P"]),
        int(row["MARG_HH_M"]),
        int(row["MARG_HH_F"]),
        int(row["MARG_OT_P"]),
        int(row["MARG_OT_M"]),
        int(row["MARG_OT_F"]),
        int(row["MARGWORK_3_6_P"]),
        int(row["MARGWORK_3_6_M"]),
        int(row["MARGWORK_3_6_F"]),
        int(row["MARG_CL_3_6_P"]),
        int(row["MARG_CL_3_6_M"]),
        int(row["MARG_CL_3_6_F"]),
        int(row["MARG_AL_3_6_P"]),
        int(row["MARG_AL_3_6_M"]),
        int(row["MARG_AL_3_6_F"]),
        int(row["MARG_HH_3_6_P"]),
        int(row["MARG_HH_3_6_M"]),
        int(row["MARG_HH_3_6_F"]),
        int(row["MARG_OT_3_6_P"]),
        int(row["MARG_OT_3_6_M"]),
        int(row["MARG_OT_3_6_F"]),
        int(row["MARGWORK_0_3_P"]),
        int(row["MARGWORK_0_3_M"]),
        int(row["MARGWORK_0_3_F"]),
        int(row["MARG_CL_0_3_P"]),
        int(row["MARG_CL_0_3_M"]),
        int(row["MARG_CL_0_3_F"]),
        int(row["MARG_AL_0_3_P"]),
        int(row["MARG_AL_0_3_M"]),
        int(row["MARG_AL_0_3_F"]),
        int(row["MARG_HH_0_3_P"]),
        int(row["MARG_HH_0_3_M"]),
        int(row["MARG_HH_0_3_F"]),
        int(row["MARG_OT_0_3_P"]),
        int(row["MARG_OT_0_3_M"]),
        int(row["MARG_OT_0_3_F"]),
    )
    print(f"✅ Inserted marginal_workers data for location_id {location_id}")

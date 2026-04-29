from app.infrastructure.database.db_connector import *
from app.services.telegram_bot_chat_service import telegram_send_document

def _log_technical_summary(df, log_file="tech_log.txt"):


    def f2(x):
        try: 
            if x is None:
                return "-"
            x = str(x).replace(",", ".")
            return f"{float(x):.2f}"
        except: return "-"

    def f_int(x):
        try:
            if x is None:
                return "-"
            return f"{int(float(x)):,}".replace(",", ".")
        except:
            return "-"

    df["RANK"] = pd.to_numeric(df["RANK"], errors="coerce")
    df.loc[df["RANK"] == 99999, "RANK"] = pd.NA

    with open(log_file, "w", encoding="utf-8") as f:

        df_base = (
            df.sort_values(["CREATED_DAY","RANK"], ascending=[True, True], na_position="last")
            .drop_duplicates(subset=["EXCHANGE","SYMBOL","CREATED_DAY"])
        )
        
        for _, base_row in df_base.iterrows():

            g = df[
                (df["EXCHANGE"]==base_row["EXCHANGE"]) &
                (df["SYMBOL"]==base_row["SYMBOL"]) &
                (df["CREATED_DAY"]==base_row["CREATED_DAY"])
            ].copy()

            g = g.sort_values("FRVP_HIGHEST_DATE", ascending=False)
            g_filtered = g.dropna(subset=["AVG_POC","STOP_LOSS","TARGET_PRICE"], how="all")

            if not g_filtered.empty:
                r = g_filtered.iloc[0]
            else:
                r = g.iloc[0]

            symbol = r["SYMBOL"]
            day = r["CREATED_DAY"]

            # =========================
            # CLUSTER
            # =========================
            g_unique = g.drop_duplicates(subset=["FRVP_HIGHEST_DATE"])
            cluster_count = len(g_unique)

            poc_lines = []
            poc_score_lines = []

            def calc_cluster_bonus(g):
            
                def is_close(a, b, tol=0.02):
                    return abs(a - b) / min(a, b) <= tol
            
                def cluster_bonus(g: pd.DataFrame):
                
                    g_unique = g.drop_duplicates(subset=["FRVP_HIGHEST_DATE"]).copy()
                    pocs = g_unique["FRVP_POC"].dropna().astype(float).values
                    count = len(pocs)

                    if count < 2:
                        return 0

                    # pairwise 
                    close_matrix = [
                        [is_close(pocs[i], pocs[j]) for j in range(count)]
                        for i in range(count)
                    ]

                    close_counts = [sum(row) for row in close_matrix]
                    max_cluster = max(close_counts)

                    if count >= 4:
                        if max_cluster >= 4:
                            return 15
                        elif max_cluster == 3:
                            return 12
                        elif max_cluster == 2:
                            return 10

                    elif count == 3:
                        if max_cluster == 3:
                            return 10
                        elif max_cluster == 2:
                            return 8

                    elif count == 2:
                        if max_cluster == 2:
                            return 7

                    return 0
            
            

            for _, row in g_unique.iterrows():

                close = float(row["FRVP_LATEST_CLOSE_VALUE"])
                poc = float(row["FRVP_POC"])
                val = float(row["FRVP_VAL"])
                vah = float(row["FRVP_VAH"])

                value_range = vah - val

                # score1
                dist_close_poc = abs(close - poc)
                if dist_close_poc <= value_range * 0.05:
                    s1 = 10
                elif dist_close_poc <= value_range * 0.80:
                    s1 = (1 - (dist_close_poc - value_range*0.05)/(value_range*0.75)) * 10
                else:
                    s1 = 0

                # score3
                s3 = 5 if row["BS_BAR_STATUS"]=="GREEN" else 0

                s2 = calc_cluster_bonus(g) or 0

                # score4
                s4 = 5 if (float(row["BS_OPEN_PRICE"])>poc and row["BS_BAR_STATUS"]=="GREEN") else 0

                # score5
                dist_poc_val = abs(poc - val)
                if dist_poc_val <= value_range*0.10:
                    s5 = 5
                elif dist_poc_val <= value_range*0.40:
                    s5 = (1-(dist_poc_val-value_range*0.10)/(value_range*0.30))*5
                else:
                    s5 = 0

                total = s1+s2+s3+s4+s5
                final = (total/40)*100

                poc_score_lines.append(f"""
PERIOD: {row["FRVP_PERIOD_TYPE"]}
HIGHEST_DATE: {row["FRVP_HIGHEST_DATE"]}

VAL: {f2(val)} | POC: {f2(poc)} | VAH: {f2(vah)}
VWAP: {f2(row["VWAP"])}

score1 (close→poc): {f2(s1)}
score2 (cluster): {f2(s2)}
score3 (green): {f2(s3)}
score4 (contactless): {f2(s4)}
score5 (poc→val): {f2(s5)}

TOTAL: {f2(total)} → FINAL_poc puan: {f2(final)}

📊 VWAP
VWAP_SCORE: {f2(r.get("VWAP_STATUS"))}

{row["FRVP_PERIOD_TYPE"]} PERIOD_MASTER_SCORE: {f2(row.get("MASTER_SCORE"))}

-------------------------------------------
""")

                poc_score_text = "\n".join(poc_score_lines)

            rank_val = "-" if pd.isna(r.get("RANK")) else int(float(r["RANK"]))
            avg_poc = f2(r.get("AVG_POC"))
            days_after = int(r.get("DAYS_AFTER_POC")) if pd.notna(r.get("DAYS_AFTER_POC")) else "-"
            stop = f2(r.get("STOP_LOSS"))
            target = f2(r.get("TARGET_PRICE"))
            triage_val = f2(r.get("TRIAGE_SCORE"))
            open_price = f2(r.get("BS_OPEN_PRICE"))
            close_price = f2(r.get("BS_CLOSE_PRICE"))
            last_price = f2(r.get("FRVP_LATEST_CLOSE_VALUE"))
            pivot = f2(r.get("PIVOT"))
            r1 = f2(r.get("PVT_R1"))
            r2 = f2(r.get("PVT_R2"))
            r4 = f2(r.get("PVT_R4"))
            s1 = f2(r.get("PVT_S1"))
            s2 = f2(r.get("PVT_S2"))
            s4 = f2(r.get("PVT_S4"))

            f.write(f"""
===================== {symbol} =====================

📅 Gün: {day}

🏆 TRADEOPS_RANK: {rank_val} | TRIAGE_SCORE: {triage_val}
📊 AVG_POC: {avg_poc} | DAYS_AFTER_POC: {days_after}
🎯 TARGET: {target} | 🛑 STOP: {stop}
📌 PRICE
Open: {open_price} | Close: {close_price} | Last: {last_price}

📊 Cluster: {cluster_count}

🧠 POC SCORE DETAIL
{poc_score_text}

📉 EMA
EMA_SCORE: {f2(r.get("EMA_STATUS"))}
EMA3: {f2(r.get("EMA3"))} | EMA5: {f2(r.get("EMA5"))} | EMA14: {f2(r.get("EMA14"))} | EMA20: {f2(r.get("EMA20"))}
EMA 5-20 Status: {r.get("EMA_STATUS_5_20")} | Cross: {r.get("EMA_CROSS_5_20")} | Days: {r.get("EMA_DAYS_SINCE_CROSS_5_20")}
EMA 3-20 Status: {r.get("EMA_STATUS_3_20")} | Cross: {r.get("EMA_CROSS_3_20")} | Days: {r.get("EMA_DAYS_SINCE_CROSS_3_20")}
EMA 3-14 Status: {r.get("EMA_STATUS_3_14")} | Cross: {r.get("EMA_CROSS_3_14")} | Days: {r.get("EMA_DAYS_SINCE_CROSS_3_14")}

📈 RSI
RSI_SCORE: {r.get("RSI_STATUS_TXT")}
RSI: {f2(r.get("RSI"))} | RSI_MA: {f2(r.get("RSI_MA"))}
Status: {r.get("RSI_STATUS")} | Cross: {r.get("RSI_CROSS")} | Days Ago: {r.get("RSI_CROSS_DAYS_AGO")}

💸 MFI
MFI_SCORE: {r.get("MFI_STATUS")}
MFI: {f2(r.get("MFI"))} | Yesterday: {f2(r.get("MFI_YESTERDAY"))} | Avg12: {f2(r.get("MFI_12DAY_AVG"))}
Direction: {r.get("MFI_DIRECTION")}

📦 VOLUME
VOL_SCORE: {r.get("VOL_STATUS_TXT")}
Last: {f_int(r.get("VOL_LASTDAY"))} | Yesterday: {f_int(r.get("VOL_YESTERDAY"))} |Avg5: {r.get(f_int("VOL_AVG_5DAY"))} | Avg10: {f_int(r.get("VOL_AVG_10DAY"))} | Avg20: {f_int(r.get("VOL_AVG_20DAY"))}

📊 PIVOT
PIVOT: {pivot}
R1: {r1} | R2: {r2} | R4: {r4}
S1: {s1} | S2: {s2} | S4: {s4}

------------------------------------------------------------
""")
            
    return log_file

def trigger_detailed_log(log_file="tech_log.txt",schema='gold',table='bist_master_final_combined'):
    df = fn_read_data_cloud(schema,table)
    log_file = _log_technical_summary(df,log_file=log_file)
    telegram_send_document(log_file, title="TECH LOG")





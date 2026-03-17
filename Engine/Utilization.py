import pandas as pd

# load data
schedule = pd.read_csv("database/schedule_final.csv")
downtime = pd.read_csv("database/planned_downtime_schedule.csv")

# convert column names
schedule.columns = schedule.columns.str.lower()
downtime.columns = downtime.columns.str.lower()

MINUTES_PER_DAY = 1440

conflicts = []

for _, row in downtime.iterrows():

    line = row["line_id"]
    day = row["day"]
    start = row["start_min"]
    end = row["end_min"]

    abs_start = day * MINUTES_PER_DAY + start
    abs_end = day * MINUTES_PER_DAY + end

    # check overlap
    overlap_jobs = schedule[
        (schedule["line_id"] == line) &
        (schedule["start_min"] < abs_end) &
        (schedule["end_min"] > abs_start)
    ]

    if not overlap_jobs.empty:
        conflicts.append((line, day, overlap_jobs))

# print result
if len(conflicts) == 0:
    print("✅ NO DOWNTIME CONFLICTS")
else:
    print("❌ CONFLICT FOUND:")
    for c in conflicts:
        print(c[0], "day", c[1])
        print(c[2])


import pandas as pd

MINUTES_PER_DAY = 1440
PLANNING_DAYS = 28

# =========================
# LOAD DATA
# =========================
schedule = pd.read_csv("database/schedule_final.csv")
downtime = pd.read_csv("database/planned_downtime_schedule.csv")
machines = pd.read_csv("data/factories_capacities.csv")

schedule.columns = schedule.columns.str.lower()
downtime.columns = downtime.columns.str.lower()
machines.columns = machines.columns.str.lower()

# =========================
# TOTAL TIME
# =========================
TOTAL_TIME = PLANNING_DAYS * MINUTES_PER_DAY

# =========================
# LINE UTILIZATION
# =========================
line_util = []

for line in machines["line_id"].unique():

    # total running time
    line_jobs = schedule[schedule["line_id"] == line]
    run_time = (line_jobs["end_min"] - line_jobs["start_min"]).sum()

    # planned downtime
    line_pd = downtime[downtime["line_id"] == line]
    pd_time = (line_pd["end_min"] - line_pd["start_min"]).sum()

    # available time
    available_time = TOTAL_TIME - pd_time

    utilization = run_time / available_time if available_time > 0 else 0

    line_util.append({
        "line_id": line,
        "run_time": run_time,
        "available_time": available_time,
        "utilization": utilization
    })

line_util_df = pd.DataFrame(line_util)

# =========================
# FACTORY UTILIZATION
# =========================
factory_util = []

for factory in machines["factory_id"].unique():

    lines = machines[machines["factory_id"] == factory]["line_id"]

    run_time = line_util_df[line_util_df["line_id"].isin(lines)]["run_time"].sum()
    available_time = line_util_df[line_util_df["line_id"].isin(lines)]["available_time"].sum()

    utilization = run_time / available_time if available_time > 0 else 0

    factory_util.append({
        "factory_id": factory,
        "utilization": utilization
    })

factory_util_df = pd.DataFrame(factory_util)

# =========================
# OUTPUT
# =========================
print("\n=== LINE UTILIZATION ===")
print(line_util_df)

print("\n=== FACTORY UTILIZATION ===")
print(factory_util_df)

# save
line_util_df.to_csv("database/line_utilization.csv", index=False)
factory_util_df.to_csv("database/factory_utilization.csv", index=False)
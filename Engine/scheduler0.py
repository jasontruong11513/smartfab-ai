import pandas as pd

MINUTES_PER_DAY = 1440
DAYS_TO_PLAN = 28
MIN_GAP_DAYS = 10

# Fixed blocked windows every day
BLOCKED_WINDOWS = [
    (180, 210),    # 03:00-03:30
    (360, 390),    # 06:00-06:30
    (600, 630),    # 10:00-10:30
    (900, 930),    # 15:00-15:30
    (1080, 1110),  # 18:00-18:30
    (1320, 1350),  # 22:00-22:30
]

# Keep candidate set small for speed
CANDIDATE_DAYS = [1, 3, 5, 7, 10, 14, 18, 21, 24, 26]
CANDIDATE_SLOTS = [
    (11 * 60, 12 * 60),
    (13 * 60, 14 * 60),
    (16 * 60, 17 * 60),
]

def load_data():
    jobs = pd.read_csv("database/generated_jobs.csv")
    routing = pd.read_csv("database/job_process_flow.csv")
    machines = pd.read_csv("data/factories_capacities.csv")
    factory_map = pd.read_csv("database/product_factory_assignment.csv")
    changeover = pd.read_csv("data/changeover.csv")
    downtime = pd.read_csv("data/downtime.csv")
    product_master = pd.read_csv("data/Produc_Master_file.csv")

    for df in [jobs, routing, machines, factory_map, changeover, downtime, product_master]:
        df.columns = df.columns.str.lower().str.strip()

    return jobs, routing, machines, factory_map, changeover, downtime, product_master


def overlap(a, b):
    return not (a[1] <= b[0] or a[0] >= b[1])


def push_after_blocked(start, duration, blocked):
    s = start
    while True:
        e = s + duration
        moved = False
        for b in blocked:
            if overlap((s, e), b):
                s = b[1]
                moved = True
                break
        if not moved:
            return s, e


def build_blocks_for_line(line_id, pd_choice):
    blocks = []

    for day in range(DAYS_TO_PLAN):
        offset = day * MINUTES_PER_DAY
        for s, e in BLOCKED_WINDOWS:
            blocks.append((offset + s, offset + e))

    for day, s, e in pd_choice.get(line_id, []):
        offset = day * MINUTES_PER_DAY
        blocks.append((offset + s, offset + e))

    return sorted(blocks)


def build_family_map(product_master):
    if "family" in product_master.columns:
        return dict(zip(product_master["product_id"], product_master["family"]))
    return dict(zip(product_master["product_id"], ["GEN"] * len(product_master)))


def build_changeover_lookup(changeover_df):
    lookup = {}
    for _, row in changeover_df.iterrows():
        lookup[row["line_id"]] = {
            "same_product": float(row["same_product_min"]),
            "same_family": float(row["same_family_min"]),
            "different_family": float(row["different_family_min"]),
        }
    return lookup


def get_changeover_time(line_id, prev_product, curr_product, family_map, co_lookup):
    if prev_product is None:
        return 0.0
    if prev_product == curr_product:
        return co_lookup[line_id]["same_product"]
    if family_map.get(prev_product, "GEN") == family_map.get(curr_product, "GEN"):
        return co_lookup[line_id]["same_family"]
    return co_lookup[line_id]["different_family"]


def simulate_schedule(jobs, routing, machines, factory_map, co_lookup, family_map, pd_choice):
    blocked_by_line = {
        line_id: build_blocks_for_line(line_id, pd_choice)
        for line_id in machines["line_id"].unique()
    }

    machine_available = {line_id: 0.0 for line_id in machines["line_id"].unique()}
    machine_last_product = {line_id: None for line_id in machines["line_id"].unique()}

    schedule_rows = []

    jobs_sorted = jobs.sort_values(["deadline_day", "product_id", "job_id"])

    for _, job in jobs_sorted.iterrows():
        job_id = job["job_id"]
        product_id = job["product_id"]

        assigned_factory = factory_map[
            factory_map["product_id"] == product_id
        ]["assigned_factory"].values[0]

        steps = routing[routing["job_id"] == job_id].sort_values("step")
        prev_end = 0.0

        for _, step in steps.iterrows():
            process_name = step["process_name"]

            candidate_lines = machines[
                (machines["process"] == process_name) &
                (machines["factory_id"] == assigned_factory)
            ]

            if candidate_lines.empty:
                raise ValueError(
                    f"No candidate line for process={process_name}, factory={assigned_factory}, job={job_id}"
                )

            best_line = None
            best_start = None
            best_end = None
            best_co = None

            for _, machine in candidate_lines.iterrows():
                line_id = machine["line_id"]
                duration = float(machine["processing_time"])

                co_time = get_changeover_time(
                    line_id,
                    machine_last_product[line_id],
                    product_id,
                    family_map,
                    co_lookup
                )

                earliest_start = max(prev_end, machine_available[line_id]) + co_time
                start, end = push_after_blocked(
                    earliest_start,
                    duration,
                    blocked_by_line[line_id]
                )

                if best_end is None or end < best_end:
                    best_line = line_id
                    best_start = start
                    best_end = end
                    best_co = co_time

            schedule_rows.append({
                "job_id": job_id,
                "product_id": product_id,
                "step": int(step["step"]),
                "process_name": process_name,
                "line_id": best_line,
                "start_min": best_start,
                "end_min": best_end,
                "changeover_time": best_co
            })

            machine_available[best_line] = best_end
            machine_last_product[best_line] = product_id
            prev_end = best_end

    return pd.DataFrame(schedule_rows)


def compute_score(schedule_df, jobs_df):
    completion = schedule_df.groupby("job_id")["end_min"].max().to_dict()

    total_tardiness = 0.0
    for _, row in jobs_df.iterrows():
        due = float(row["deadline_day"]) * MINUTES_PER_DAY
        finish = completion.get(row["job_id"], due)
        total_tardiness += max(0.0, finish - due)

    total_changeover = schedule_df["changeover_time"].sum()
    makespan = schedule_df["end_min"].max() if not schedule_df.empty else 0.0

    # deadline là quan trọng nhất
    return total_tardiness * 1000 + total_changeover * 10 + makespan


def get_line_process_map(machines):
    return dict(zip(machines["line_id"], machines["process"]))


def has_same_process_conflict(line_id, slot_tuple, current_choice, line_process_map):
    process_name = line_process_map[line_id]

    for other_line, slots in current_choice.items():
        if other_line == line_id:
            continue
        if line_process_map[other_line] != process_name:
            continue
        if slot_tuple in slots:
            return True

    return False


def choose_first_pd_slot_for_line(line_id, jobs, routing, machines, factory_map, co_lookup, family_map, current_choice, line_process_map):
    best_slot = None
    best_score = None

    for day in CANDIDATE_DAYS:
        for slot in CANDIDATE_SLOTS:
            slot_tuple = (day, slot[0], slot[1])

            if has_same_process_conflict(line_id, slot_tuple, current_choice, line_process_map):
                continue

            trial_choice = {k: list(v) for k, v in current_choice.items()}
            trial_choice[line_id] = [slot_tuple]

            schedule_df = simulate_schedule(
                jobs, routing, machines, factory_map, co_lookup, family_map, trial_choice
            )
            score = compute_score(schedule_df, jobs)

            # penalty nhỏ để tránh downtime quá sớm
            score += max(0, 5 - day) * 50

            if best_score is None or score < best_score:
                best_score = score
                best_slot = slot_tuple

    return best_slot, best_score


def choose_second_pd_slot_for_line(line_id, first_slot, jobs, routing, machines, factory_map, co_lookup, family_map, current_choice, line_process_map):
    best_slot = None
    best_score = None

    for day in CANDIDATE_DAYS:
        if abs(day - first_slot[0]) < MIN_GAP_DAYS:
            continue

        for slot in CANDIDATE_SLOTS:
            slot_tuple = (day, slot[0], slot[1])

            if has_same_process_conflict(line_id, slot_tuple, current_choice, line_process_map):
                continue

            trial_choice = {k: list(v) for k, v in current_choice.items()}
            trial_choice[line_id] = [first_slot, slot_tuple]

            schedule_df = simulate_schedule(
                jobs, routing, machines, factory_map, co_lookup, family_map, trial_choice
            )
            score = compute_score(schedule_df, jobs)

            if best_score is None or score < best_score:
                best_score = score
                best_slot = slot_tuple

    return best_slot, best_score


def choose_pd_fast(jobs, routing, machines, factory_map, co_lookup, family_map):
    pd_choice = {}
    line_process_map = get_line_process_map(machines)

    # sort lines: bottleneck-looking lines first (single-line processes first)
    process_counts = machines.groupby("process")["line_id"].count().to_dict()
    all_lines = list(machines["line_id"].unique())
    all_lines.sort(key=lambda x: process_counts[line_process_map[x]])

    for line_id in all_lines:
        first_slot, score1 = choose_first_pd_slot_for_line(
            line_id, jobs, routing, machines, factory_map, co_lookup, family_map, pd_choice, line_process_map
        )

        if first_slot is None:
            raise ValueError(f"Cannot find first planned downtime slot for line {line_id}")

        pd_choice[line_id] = [first_slot]

        second_slot, score2 = choose_second_pd_slot_for_line(
            line_id, first_slot, jobs, routing, machines, factory_map, co_lookup, family_map, pd_choice, line_process_map
        )

        if second_slot is None:
            raise ValueError(f"Cannot find second planned downtime slot for line {line_id}")

        pd_choice[line_id] = [first_slot, second_slot]

        print(f"[PD] {line_id} -> {pd_choice[line_id]}")

    return pd_choice


def apply_yield_downtime(schedule_df, downtime_df):
    schedule_df = schedule_df.sort_values(["line_id", "start_min"]).copy()
    result = []

    for line_id, group in schedule_df.groupby("line_id"):
        group = group.sort_values("start_min").reset_index(drop=True)

        rule = downtime_df[downtime_df["line_id"] == line_id]
        if rule.empty:
            for _, row in group.iterrows():
                result.append(row.to_dict())
            continue

        avg_interval_min = float(rule["avg_yield_interval_hr"].values[0]) * 60.0
        fix_time_min = float(rule["avg_fix_time_min"].values[0])

        running_since_fix = 0.0
        shift_delta = 0.0
        last_end = None

        for _, row in group.iterrows():
            r = row.copy()

            start = float(r["start_min"]) + shift_delta
            end = float(r["end_min"]) + shift_delta

            if last_end is not None and start > last_end:
                running_since_fix = 0.0

            duration = end - start

            if running_since_fix + duration >= avg_interval_min and duration >= 10:
                end += fix_time_min
                shift_delta += fix_time_min
                running_since_fix = 0.0
            else:
                running_since_fix += duration

            r["start_min"] = start
            r["end_min"] = end
            result.append(r.to_dict())
            last_end = end

    return pd.DataFrame(result)


def save_planned_downtime(pd_choice, machines):
    rows = []
    for line_id, slots in pd_choice.items():
        process_name = machines[machines["line_id"] == line_id]["process"].values[0]
        factory_id = machines[machines["line_id"] == line_id]["factory_id"].values[0]

        for idx, slot in enumerate(slots, start=1):
            rows.append({
                "line_id": line_id,
                "factory_id": factory_id,
                "process": process_name,
                "pd_no": idx,
                "day": slot[0],
                "start_min": slot[1],
                "end_min": slot[2]
            })

    pd.DataFrame(rows).to_csv("database/planned_downtime_schedule.csv", index=False)


def main():
    print("Loading data...")
    jobs, routing, machines, factory_map, changeover, downtime, product_master = load_data()

    family_map = build_family_map(product_master)
    co_lookup = build_changeover_lookup(changeover)

    print("Choosing fast planned downtime...")
    pd_choice = choose_pd_fast(
        jobs, routing, machines, factory_map, co_lookup, family_map
    )

    print("Building baseline schedule...")
    baseline = simulate_schedule(
        jobs, routing, machines, factory_map, co_lookup, family_map, pd_choice
    )
    baseline.to_csv("database/schedule_baseline.csv", index=False)

    print("Applying yield downtime...")
    final_schedule = apply_yield_downtime(baseline, downtime)
    final_schedule.to_csv("database/schedule_final.csv", index=False)

    save_planned_downtime(pd_choice, machines)

    print("Done.")
    print("Outputs:")
    print("- database/schedule_baseline.csv")
    print("- database/schedule_final.csv")
    print("- database/planned_downtime_schedule.csv")


if __name__ == "__main__":
    main()
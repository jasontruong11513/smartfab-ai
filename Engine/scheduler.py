import pandas as pd
from datetime import datetime, timedelta

MINUTES_PER_DAY = 1440
PLANNING_DAYS = 28
START_DATE = datetime(2026, 4, 1)
MIN_GAP_DAYS = 10

# Fixed blocked windows in each day
BLOCKED_WINDOWS = [
    (180, 210),
    (360, 390),
    (600, 630),
    (900, 930),
    (1080, 1110),
    (1320, 1350),
]

# Preferred planned downtime slots per process
PROCESS_SLOT_MAP = {
    "wafer_saw": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
    "die_attach": [(13 * 60, 14 * 60), (16 * 60, 17 * 60)],
    "wire_bond": [(16 * 60, 17 * 60), (19 * 60, 20 * 60)],
    "molding": [(11 * 60, 12 * 60), (16 * 60, 17 * 60)],
    "rinse": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
    "plating": [(13 * 60, 14 * 60), (16 * 60, 17 * 60)],
    "coating": [(13 * 60, 14 * 60), (16 * 60, 17 * 60)],
    "trim_form": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
    "laser_marking": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
    "electrical_test": [(13 * 60, 14 * 60), (16 * 60, 17 * 60)],
    "final_inspection": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
    "default": [(11 * 60, 12 * 60), (13 * 60, 14 * 60)],
}


def build_candidate_days():
    """
    Keep only Monday / Tuesday / Wednesday as valid planned downtime days.
    """
    candidate_days = []
    for day in range(PLANNING_DAYS):
        current_date = START_DATE + timedelta(days=day)
        if current_date.weekday() in [0, 1, 2]:
            candidate_days.append(day)
    return candidate_days


CANDIDATE_DAYS = build_candidate_days()


def normalize_process_name(series):
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )


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

    # Normalize key fields
    machines["factory_id"] = machines["factory_id"].astype(str).str.strip().str.upper()
    factory_map["assigned_factory"] = factory_map["assigned_factory"].astype(str).str.strip().str.upper()

    if "process" in machines.columns:
        machines["process"] = normalize_process_name(machines["process"])

    if "process_name" in routing.columns:
        routing["process_name"] = normalize_process_name(routing["process_name"])

    # Force numeric
    jobs["deadline_day"] = pd.to_numeric(jobs["deadline_day"], errors="raise")
    machines["processing_time"] = pd.to_numeric(machines["processing_time"], errors="raise")

    for col in ["same_product_min", "same_family_min", "different_family_min"]:
        if col in changeover.columns:
            changeover[col] = pd.to_numeric(changeover[col], errors="raise")

    for col in ["avg_yield_interval_hr", "avg_fix_time_min"]:
        if col in downtime.columns:
            downtime[col] = pd.to_numeric(downtime[col], errors="raise")

    return jobs, routing, machines, factory_map, changeover, downtime, product_master


def overlap(a, b):
    return not (a[1] <= b[0] or a[0] >= b[1])


def push(start, dur, blocks):
    """
    Push an operation until it does not overlap ANY blocked window.
    Duration is preserved exactly.
    """
    s = float(start)
    dur = float(dur)

    while True:
        e = s + dur
        hit = None

        for b in blocks:
            if overlap((s, e), b):
                hit = b
                break

        if hit is None:
            return s, s + dur

        s = float(hit[1])


def build_blocks(line_id, pd_choice):
    blocks = []

    for day in range(PLANNING_DAYS):
        offset = day * MINUTES_PER_DAY
        for b in BLOCKED_WINDOWS:
            blocks.append((offset + b[0], offset + b[1]))

    for day, start_min, end_min in pd_choice.get(line_id, []):
        offset = day * MINUTES_PER_DAY
        blocks.append((offset + start_min, offset + end_min))

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


def get_changeover_time(line_id, prev_product, curr_product, family_map, changeover_lookup):
    if prev_product is None:
        return 0.0
    if prev_product == curr_product:
        return changeover_lookup[line_id]["same_product"]
    if family_map.get(prev_product, "GEN") == family_map.get(curr_product, "GEN"):
        return changeover_lookup[line_id]["same_family"]
    return changeover_lookup[line_id]["different_family"]


def validate_job_factory_feasibility(jobs, routing, machines, factory_map):
    """
    Ensure each job can execute ALL its processes within its assigned factory.
    If not, crash immediately instead of silently hopping factory.
    """
    product_to_factory = dict(zip(factory_map["product_id"], factory_map["assigned_factory"]))

    machine_process_by_factory = (
        machines.groupby("factory_id")["process"]
        .apply(set)
        .to_dict()
    )

    errors = []

    for _, job_row in jobs.iterrows():
        job_id = job_row["job_id"]
        product_id = job_row["product_id"]
        assigned_factory = product_to_factory[product_id]

        job_processes = set(
            routing[routing["job_id"] == job_id]["process_name"].tolist()
        )

        supported = machine_process_by_factory.get(assigned_factory, set())
        missing = sorted(list(job_processes - supported))

        if missing:
            errors.append({
                "job_id": job_id,
                "product_id": product_id,
                "assigned_factory": assigned_factory,
                "missing_processes": ",".join(missing)
            })

    if errors:
        error_df = pd.DataFrame(errors)
        error_df.to_csv("database/factory_feasibility_errors.csv", index=False)
        raise ValueError(
            "Factory feasibility failed. Some jobs require processes not available in their assigned factory. "
            "See database/factory_feasibility_errors.csv"
        )


def simulate(jobs, routing, machines, factory_map, changeover_lookup, family_map, pd_choice):
    """
    Build a baseline schedule while enforcing:
    - one job stays in exactly one factory
    - no cross-factory execution
    - duration preserved exactly
    """
    blocks = {line: build_blocks(line, pd_choice) for line in machines["line_id"].unique()}
    available_time = {line: 0.0 for line in machines["line_id"].unique()}
    last_product = {line: None for line in machines["line_id"].unique()}

    product_to_factory = dict(zip(factory_map["product_id"], factory_map["assigned_factory"]))
    job_factory_map = {
        row["job_id"]: product_to_factory[row["product_id"]]
        for _, row in jobs.iterrows()
    }

    schedule = []
    debug_rows = []

    jobs = jobs.sort_values(["deadline_day", "product_id", "job_id"])

    for _, job_row in jobs.iterrows():
        job_id = job_row["job_id"]
        product_id = job_row["product_id"]
        assigned_factory = job_factory_map[job_id]

        steps = routing[routing["job_id"] == job_id].sort_values("step")
        prev_end = 0.0

        for _, step_row in steps.iterrows():
            process_name = step_row["process_name"]

            candidate_lines = machines[
                (machines["process"] == process_name) &
                (machines["factory_id"] == assigned_factory)
            ]

            if candidate_lines.empty:
                raise ValueError(
                    f"[FATAL] No machine for job={job_id}, process={process_name}, factory={assigned_factory}"
                )

            best = None
            best_end = None

            for _, machine_row in candidate_lines.iterrows():
                line_id = machine_row["line_id"]
                duration = float(machine_row["processing_time"])

                co_time = get_changeover_time(
                    line_id,
                    last_product[line_id],
                    product_id,
                    family_map,
                    changeover_lookup
                )

                raw_start = max(prev_end, available_time[line_id]) + co_time
                start, end = push(raw_start, duration, blocks[line_id])

                delta = end - start

                debug_rows.append({
                    "job_id": job_id,
                    "product_id": product_id,
                    "process_name": process_name,
                    "assigned_factory": assigned_factory,
                    "candidate_line": line_id,
                    "candidate_factory": machine_row["factory_id"],
                    "raw_start": raw_start,
                    "duration": duration,
                    "start_after_push": start,
                    "end_after_push": end,
                    "delta": delta
                })

                if abs(delta - duration) > 1e-9:
                    raise ValueError(
                        f"Duration mismatch on line={line_id}, job={job_id}, process={process_name}, "
                        f"duration={duration}, delta={delta}"
                    )

                if best_end is None or end < best_end:
                    best_end = end
                    best = (line_id, start, end, co_time, duration)

            line_id, start, end, co_time, duration = best

            actual_factory = machines.loc[machines["line_id"] == line_id, "factory_id"].values[0]
            if actual_factory != assigned_factory:
                raise ValueError(
                    f"[ERROR] Job {job_id} switched factory. Expected {assigned_factory}, got {actual_factory}"
                )

            schedule.append({
                "job_id": job_id,
                "product_id": product_id,
                "process_name": process_name,
                "line_id": line_id,
                "factory_id": actual_factory,
                "start_min": start,
                "end_min": end,
                "changeover_time": co_time,
                "processing_time_used": duration,
                "duration_check": end - start
            })

            available_time[line_id] = end
            last_product[line_id] = product_id
            prev_end = end

    pd.DataFrame(debug_rows).to_csv("database/debug_schedule_candidates.csv", index=False)

    # Final hard validation: each job must only appear in one factory
    sched_df = pd.DataFrame(schedule)
    factory_check = sched_df.groupby("job_id")["factory_id"].nunique()
    bad_jobs = factory_check[factory_check > 1]
    if not bad_jobs.empty:
        raise ValueError(
            f"[ERROR] Some jobs run in multiple factories: {bad_jobs.index.tolist()}"
        )

    return sched_df


def has_same_process_conflict(line_id, slot_tuple, current_pd_choice, process_map):
    line_process = process_map[line_id]

    for other_line, slots in current_pd_choice.items():
        if other_line == line_id:
            continue
        if process_map[other_line] != line_process:
            continue
        if slot_tuple in slots:
            return True

    return False


def choose_pd(jobs, routing, machines, factory_map, changeover_lookup, family_map):
    """
    Keep your fast planned downtime logic.
    """
    pd_choice = {}
    process_map = dict(zip(machines["line_id"], machines["process"]))

    base_schedule = simulate(
        jobs, routing, machines, factory_map, changeover_lookup, family_map, {}
    )

    for line_id in machines["line_id"].unique():
        line_process = process_map[line_id]
        preferred_slots = PROCESS_SLOT_MAP.get(line_process, PROCESS_SLOT_MAP["default"])

        line_sched = base_schedule[base_schedule["line_id"] == line_id].sort_values("start_min")

        line_days_seen = []
        for _, row in line_sched.iterrows():
            day_idx = int(row["start_min"] // MINUTES_PER_DAY)
            if day_idx in CANDIDATE_DAYS and day_idx not in line_days_seen:
                line_days_seen.append(day_idx)

        if not line_days_seen:
            line_days_seen = CANDIDATE_DAYS.copy()

        first_slot = None
        for day in line_days_seen + CANDIDATE_DAYS:
            if day not in CANDIDATE_DAYS:
                continue
            slot_tuple = (day, preferred_slots[0][0], preferred_slots[0][1])
            if not has_same_process_conflict(line_id, slot_tuple, pd_choice, process_map):
                first_slot = slot_tuple
                break

        if first_slot is None:
            first_slot = (CANDIDATE_DAYS[0], preferred_slots[0][0], preferred_slots[0][1])

        second_slot = None
        for day in CANDIDATE_DAYS:
            if abs(day - first_slot[0]) < MIN_GAP_DAYS:
                continue
            slot_tuple = (day, preferred_slots[1][0], preferred_slots[1][1])
            if not has_same_process_conflict(line_id, slot_tuple, pd_choice, process_map):
                second_slot = slot_tuple
                break

        if second_slot is None:
            fallback_day = first_slot[0] + MIN_GAP_DAYS
            if fallback_day >= PLANNING_DAYS:
                fallback_day = first_slot[0] - MIN_GAP_DAYS
            second_slot = (fallback_day, preferred_slots[1][0], preferred_slots[1][1])

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


def save_planned_downtime_output(pd_choice, machines):
    rows = []

    for line_id, slots in pd_choice.items():
        process_name = machines[machines["line_id"] == line_id]["process"].values[0]
        factory_id = machines[machines["line_id"] == line_id]["factory_id"].values[0]

        for idx, slot in enumerate(slots, start=1):
            day, start_min, end_min = slot
            abs_start = day * MINUTES_PER_DAY + start_min
            abs_end = day * MINUTES_PER_DAY + end_min

            rows.append({
                "line_id": line_id,
                "factory_id": factory_id,
                "process": process_name,
                "pd_no": idx,
                "day": day,
                "start_min": start_min,
                "end_min": end_min,
                "start_time": START_DATE + timedelta(minutes=int(abs_start)),
                "end_time": START_DATE + timedelta(minutes=int(abs_end)),
            })

    pd.DataFrame(rows).to_csv("database/planned_downtime_schedule.csv", index=False)


def main():
    jobs, routing, machines, factory_map, changeover, downtime, product_master = load_data()

    family_map = build_family_map(product_master)
    changeover_lookup = build_changeover_lookup(changeover)

    # Validate factory feasibility BEFORE scheduling
    validate_job_factory_feasibility(jobs, routing, machines, factory_map)

    print("Selecting planned downtime...")
    pd_choice = choose_pd(
        jobs, routing, machines, factory_map, changeover_lookup, family_map
    )

    print("Building baseline schedule...")
    baseline = simulate(
        jobs, routing, machines, factory_map, changeover_lookup, family_map, pd_choice
    )
    baseline["start_time"] = baseline["start_min"].apply(
        lambda x: START_DATE + timedelta(minutes=int(x))
    )
    baseline["end_time"] = baseline["end_min"].apply(
        lambda x: START_DATE + timedelta(minutes=int(x))
    )
    baseline.to_csv("database/schedule_baseline.csv", index=False)

    print("Applying yield downtime...")
    final_schedule = apply_yield_downtime(baseline, downtime)
    final_schedule["start_time"] = final_schedule["start_min"].apply(
        lambda x: START_DATE + timedelta(minutes=int(x))
    )
    final_schedule["end_time"] = final_schedule["end_min"].apply(
        lambda x: START_DATE + timedelta(minutes=int(x))
    )

    # Deadline check by job
    violations = []

    for job_id, group in final_schedule.groupby("job_id"):
        finish_time = group["end_min"].max()
        deadline_day = jobs[jobs["job_id"] == job_id]["deadline_day"].values[0]
        deadline_time = deadline_day * MINUTES_PER_DAY
        lateness = finish_time - deadline_time

        if lateness > 0:
            violations.append({
                "job_id": job_id,
                "lateness_min": lateness,
                "lateness_hours": lateness / 60
            })

    violations_df = pd.DataFrame(violations)
    if not violations_df.empty:
        print("\nWARNING: DEADLINE VIOLATIONS DETECTED")
        print(violations_df)
    else:
        print("\nALL JOBS MEET DEADLINE")

    violations_df.to_csv("database/deadline_violations.csv", index=False)

    final_schedule = final_schedule.sort_values(["line_id", "start_min"]).reset_index(drop=True)
    final_schedule.to_csv("database/schedule_final.csv", index=False)

    save_planned_downtime_output(pd_choice, machines)

    print("DONE")
    print("Outputs generated:")
    print("- database/schedule_baseline.csv")
    print("- database/schedule_final.csv")
    print("- database/planned_downtime_schedule.csv")
    print("- database/debug_schedule_candidates.csv")
    print("- database/deadline_violations.csv")


if __name__ == "__main__":
    main()
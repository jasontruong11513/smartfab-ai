import pandas as pd


def build_process_flow(product, matrix_row, processes):

    flow = []

    base_flow = [
        "Wafer_Saw",
        "Die_Attach",
        "Wire_Bond",
        "Molding"
    ]

    flow.extend(base_flow)

    if matrix_row["Rinse"] == 1:
        flow.append("Rinse")

    if matrix_row["Plating"] == 1:
        flow.append("Plating")

    if matrix_row["Coating"] == 1:
        flow.append("Coating")

    flow.append("Trim_Form")

    if matrix_row["Laser_Marking"] == 1:
        flow.append("Laser_Marking")

    flow.extend([
        "Electrical_Test",
        "Final_Inspection"
    ])

    return flow


def generate_routing(jobs_path, matrix_path):

    jobs_df = pd.read_csv(jobs_path)
    matrix_df = pd.read_csv(matrix_path)

    routing = []

    for _, job in jobs_df.iterrows():

        job_id = job["job_id"]
        product = job["product_id"]

        matrix_row = matrix_df[matrix_df["product"] == product].iloc[0]

        flow = build_process_flow(product, matrix_row, None)

        for step, process in enumerate(flow, start=1):

            routing.append({
                "job_id": job_id,
                "step": step,
                "process_name": process
            })

    routing_df = pd.DataFrame(routing)

    return routing_df


if __name__ == "__main__":

    routing = generate_routing(
        "database/generated_jobs.csv",
        "data/production_matrix.csv"
    )

    print(routing.head())

    routing.to_csv("database/job_process_flow.csv", index=False)
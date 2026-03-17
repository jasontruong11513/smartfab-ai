import pandas as pd
import math
from datetime import datetime

PLANNING_START_DATE = datetime(2026, 4, 1) #demo only, should input by user


def generate_jobs(demand_path, product_master_path):

    demand_df = pd.read_csv(demand_path)
    product_df = pd.read_csv(product_master_path)

    demand_df.columns = demand_df.columns.str.strip().str.lower()
    product_df.columns = product_df.columns.str.strip().str.lower()

    jobs = []
    job_counter = 1000

    for _, row in demand_df.iterrows():

        product = row["product_id"]
        demand = int(row["demand"])

        # parse deadline date
        deadline_date = pd.to_datetime(row["deadline_day"])

        deadline_day = (deadline_date - PLANNING_START_DATE).days

        product_row = product_df[product_df["product_id"] == product]

        if product_row.empty:
            raise ValueError(f"Product {product} not found in Product_Master_file")

        lot_size = int(product_row["lot_size"].values[0])

        num_jobs = math.ceil(demand / lot_size)

        for i in range(num_jobs):

            job_counter += 1

            jobs.append({
                "job_id": f"J{job_counter}",
                "product_id": product,
                "quantity": lot_size,
                "deadline_day": deadline_day
            })

    jobs_df = pd.DataFrame(jobs)

    return jobs_df


if __name__ == "__main__":

    jobs = generate_jobs(
        "data/demand.csv",
        "data/Produc_Master_file.csv"
    )

    print(jobs.head())

    jobs.to_csv("database/generated_jobs.csv", index=False)
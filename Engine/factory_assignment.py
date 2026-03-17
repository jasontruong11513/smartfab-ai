import pandas as pd


def generate_product_factory_assignment(matrix_path):
    matrix_df = pd.read_csv(matrix_path)
    matrix_df.columns = matrix_df.columns.str.strip().str.lower()

    assignments = []

    for _, row in matrix_df.iterrows():
        product = row["product"]

        rinse = int(row.get("rinse", 0))
        plating = int(row.get("plating", 0))
        coating = int(row.get("coating", 0))
        laser = int(row.get("laser_marking", row.get("laser_mark", 0)))

        if plating == 1:
            assigned_factory = "F1"
            reason = "requires_plating"
        elif coating == 1 or laser == 1:
            assigned_factory = "F2"
            reason = "requires_coating_or_laser"
        else:
            assigned_factory = "F1"
            reason = "default_shared"

        assignments.append({
            "product_id": product,
            "assigned_factory": assigned_factory,
            "reason": reason
        })

    return pd.DataFrame(assignments)


if __name__ == "__main__":
    assignment_df = generate_product_factory_assignment(
        "data/production_matrix.csv"
    )

    print(assignment_df)
    assignment_df.to_csv("database/product_factory_assignment.csv", index=False)
import sqlite3
import pandas as pd


def insert_products(df, conn):
    """
    Inserts unique products from df into the 'product' table.
    Returns a mapping from product name to product id.
    """
    cur = conn.cursor()

    # Get existing products from DB
    cur.execute("SELECT id, name FROM product")
    existing_products = {name: pid for pid, name in cur.fetchall()}

    product_map = existing_products.copy()

    # Insert new products
    for product in df['product'].unique():
        if product not in existing_products:
            cur.execute("INSERT INTO product (name) VALUES (?)", (product,))
            product_id = cur.lastrowid
            product_map[product] = product_id

    conn.commit()
    return product_map


def insert_shipments(df, product_map, conn):
    """
    Inserts shipments from df into the 'shipment' table.
    """
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
            (
                product_map[row['product']],
                row['quantity'],
                row['origin_warehouse'],
                row['destination_store']
            )
        )
    conn.commit()


def main():
    # Connect to SQLite database
    conn = sqlite3.connect("shipment_database.db")

    # Load CSVs
    df0 = pd.read_csv("data/shipping_data_0.csv")
    df1 = pd.read_csv("data/shipping_data_1.csv")
    df2 = pd.read_csv("data/shipping_data_2.csv")

    # Clean column names
    for df in [df0, df1, df2]:
        df.columns = df.columns.str.strip().str.lower()

    # Rename columns to match database
    df0 = df0.rename(columns={"product_quantity": "quantity"})
    df2 = df2.rename(columns={"origin_warehouse": "origin_warehouse"})

    # Insert products from all CSVs
    combined_products = pd.concat([df0[['product']], df1[['product']]]).drop_duplicates()
    product_map = insert_products(combined_products, conn)

    # Insert shipments from shipping_data_0.csv
    insert_shipments(df0, product_map, conn)

    # Combine shipping_data_1 and shipping_data_2
    df1_merged = df1.merge(df2, on='shipment_identifier', how='left')

    # Compute quantity for each product in shipment 1 if needed
    if 'quantity' not in df1_merged.columns:
        df1_merged['quantity'] = 1  # default to 1 if no quantity column

    # Insert shipments from merged df1_merged
    insert_shipments(df1_merged, product_map, conn)

    conn.close()
    print("Database population complete!")


if __name__ == "__main__":
    main()
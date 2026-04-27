from airflow.sdk import dag, task, task_group
import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = "/opt/airflow/raw"
STAGE_DIR = "/opt/airflow/stage"
LOAD_DIR = "/opt/airflow/load"



@dag(schedule="@daily")

def test_dag():

 @task_group(group_id="extract")
 def extract():
 
  @task
  def extract_customers():
   df_customers = (
   pd.read_csv(
    f"{RAW_DIR}/customers.csv",
     dtype={
     "CustomerID" : "Int64",
     "customer_type": "string"}
    )
   .rename(columns={"CustomerID": "customer_id"})
   )
   
   output_dir = Path(f"{STAGE_DIR}/main")
   output_dir.mkdir(parents=True, exist_ok=True)
   
   path = output_dir / "customers.csv"
   df_customers.to_csv(path, index=False)
   
   return str(path)

  @task
  def extract_products():
   
   df_products = (
    pd.read_csv(
     f"{RAW_DIR}/products.csv", 
     dtype={
      "product_id" : "Int64",
      "item": "string",
      "category": "string",
      "price": "float"}
     )
    )
   
   output_dir = Path(f"{STAGE_DIR}/main")
   output_dir.mkdir(parents=True, exist_ok=True)

   path = output_dir / "products.csv"
   df_products.to_csv(path, index=False)

   return str(path)

  @task
  def extract_purchases():
   df_purchases = (
    pd.read_csv(
     f"{RAW_DIR}//purchases.csv",
     dtype={
      "InvoiceID" : "Int64",
      "product_id": "Int64",
      "CustomerID" : "Int64",
      "quantity": "Int64"},
      parse_dates=["date"])
    .rename(columns={"InvoiceID" : "invoice_id", 
                       "date" : "invoice_date", 
                       "CustomerID" : "customer_id"}))
   
   df_purchases = df_purchases.groupby(
    ["invoice_id","product_id", "invoice_date", "customer_id"], as_index=False)["quantity"].sum()
   
   output_dir = Path(f"{STAGE_DIR}/main")
   output_dir.mkdir(parents=True, exist_ok=True)

   path = output_dir / "purchases.csv"
   df_purchases.to_csv(path, index=False)

   return str(path)
  
  @task
  def extract_stage(customers_path, products_path, purchases_path, invoices_path):
   return {
   "customers" : customers_path,
   "products" : products_path,
   "purchases" : purchases_path,
   "invoices" : invoices_path,
  }

  @task
  def extract_invoices():
   df_invoices = (
    pd.read_csv(
     f"{RAW_DIR}/invoice_items.csv",
     usecols = ["InvoiceID", "product_id", "quantity", "line_total"],
     dtype={
      "InvoiceID" : "Int64",
      "product_id": "Int64",
      "quantity": "Int64",
      "line_total": "float"})
    .rename(columns={"InvoiceID" : "invoice_id"})
   )

   df_invoices = (
   df_invoices.groupby(["invoice_id", "product_id"], as_index=False)
   .agg(
    quantity=("quantity", "sum"),
    line_total=("line_total", "sum")
    )
  )
   df_invoices["price"] = (df_invoices["line_total"]/df_invoices["quantity"]).astype("float")


   output_dir = Path(f"{STAGE_DIR}/main")
   output_dir.mkdir(parents=True, exist_ok=True)

   path = output_dir / "invoices.csv"
   df_invoices.to_csv(path, index=False)

   return str(path)

  customers_path = extract_customers()
  products_path = extract_products()
  purchases_path = extract_purchases()
  invoices_path = extract_invoices()

  paths = extract_stage(customers_path, products_path, purchases_path, invoices_path)

  return paths
 
 @task_group(group_id="transform")
 def transform(paths):

  @task
  def check_nulls(paths):

   return_path = {}

   for name, path in paths.items():
    df = pd.read_csv(path)
    
    df_null = df.isna().sum().reset_index()
    df_null.columns = ["column", "null_count"]
    df_null = df_null[df_null["null_count"] > 0]

    print(f"\n{name}_table")
    print(df_null)

    output_dir = Path(f"{STAGE_DIR}/nulls")
    output_dir.mkdir(parents=True, exist_ok=True)

    return_path[name] = f"{output_dir}/{name}.csv"

    df.to_csv(return_path[name], index=False)

   return return_path


  @task
  def check_duplicates(paths):
   return_path = {}

   checks = {
     "customers" : ["customer_id"],
     "products" : ["product_id"],
     "purchases" : ["invoice_id","product_id"],
     "invoices": ["invoice_id","product_id"]
    }

   for name, keys in checks.items():
    df = pd.read_csv(paths[name])

    duplicates = df[df.duplicated(subset=keys, keep=False)]
     
    if not duplicates.empty:
     df = df.drop_duplicates(subset = keys, keep = "last")
     print(f"df_{name}_table contains duplicates, so it's prepared to keep last value of subset {keys}")
    else:
     print(f"df_{name}_table does not contain duplicates")

    output_dir = Path(f"{STAGE_DIR}/duplicates")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return_path[name] = f"{output_dir}/{name}.csv"

    df.to_csv(return_path[name], index=False)

   return return_path

  @task
  def check_anomaly(paths):
   return_path = {}

   df_invoices = pd.read_csv(paths["invoices"])
   df_products = pd.read_csv(paths["products"])


   df_price_anomaly = pd.merge(left = df_invoices, right = df_products, how = "left", on = "product_id", suffixes = ("_inv","_prd"))

   df_price_anomaly["price_flag"] = np.where(
    (df_price_anomaly["price_inv"] > 0) &
    (df_price_anomaly["price_inv"] <= df_price_anomaly["price_prd"]),
    "valid",
    "invalid"
   )

   if (df_price_anomaly["price_flag"] == "invalid").any():
    print(
        "5 first invalid invoice price ASC\n"
        f"{df_price_anomaly.loc[
            df_price_anomaly['price_flag'] == 'invalid',
            ['price_inv','price_prd']
        ].sort_values(by='price_inv', ascending=True).head(5)}"
    )
    print(
        "5 last invalid invoice price DESC\n"
        f"{df_price_anomaly.loc[
            df_price_anomaly['price_flag'] == 'invalid',
            ['price_inv','price_prd']
        ].sort_values(by='price_inv', ascending=True).tail(5)}"
    )
   else:
    print("df_invoices_table does not contain anomaly on price")

   # product в моей моделі - dim
   df_products = df_products.drop(columns=["price"])
   
   df_price_anomaly = df_price_anomaly.drop(columns=["item", "category", "price_prd"]).rename(columns={"price_inv":"price"})
   df_price_anomaly = df_price_anomaly[["invoice_id","product_id", "quantity", "line_total", "price", "price_flag"]]


   output_dir = Path(f"{STAGE_DIR}/anomaly")
   output_dir.mkdir(parents=True, exist_ok=True)

   for name, path in paths.items():
    df = pd.read_csv(path)
    
    if name == "customers" or name == "purchases":

     df.to_csv(f"{output_dir}/{name}.csv", index=False)
     return_path[name] = f"{output_dir}/{name}.csv"

    df_price_anomaly.to_csv(f"{output_dir}/invoices.csv", index=False)
    df_products.to_csv(f"{output_dir}/products.csv")
    
    return_path["invoices"] = f"{output_dir}/invoices.csv"
    return_path["products"] = f"{output_dir}/products.csv"

   return return_path
 
  @task
  def check_references(paths):
    return_path = {}

    df_purchases = pd.read_csv(paths["purchases"])
    df_customers = pd.read_csv(paths["customers"])
    df_products = pd.read_csv(paths["products"])
    df_invoices = pd.read_csv(paths["invoices"])

    checks = [
     ("purchases -> customers", df_purchases, "customer_id", df_customers),
     ("purchases -> products", df_purchases, "product_id", df_products),
     ("invoices -> products", df_invoices, "product_id", df_products),
    ]

    for name, df_left, key, df_right in checks:

     missing = df_left[~df_left[key].isin(df_right[key])]

    if not missing.empty:
     raise ValueError(
            f"{name} FAILED referential check on {key}")
    else:
        print(f"{name} PASSED referential check on {key}")
    return paths
  
  nulls_paths = check_nulls(paths)
  duplicates_paths = check_duplicates(nulls_paths)
  anomaly_paths = check_anomaly(duplicates_paths)
  reference_paths = check_references(anomaly_paths)

  return reference_paths
 
 @task_group(group_id="load")
 def load_group(paths):

  @task
  def save_files(paths):
   output_dir = Path(LOAD_DIR)
   output_dir.mkdir(parents=True, exist_ok=True)

   for name, path in paths.items():
    df = pd.read_csv(path)
    df.to_csv(output_dir / f"{name}.csv", index=False)

  save_files(paths)

 extract_paths = extract()
 transform_paths = transform(extract_paths)
 load_group(transform_paths)
 

test_dag()
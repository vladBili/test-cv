import pandas as pd
import numpy as np
import logging

from pathlib import Path  

BASE_DIR = Path(__file__).resolve().parent 


logging.basicConfig(
    filename= BASE_DIR / "test.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.warning("Task 3.1 : Завантаження та підготовка даних")

#3.1 Завантаження та підготовка даних
try:

 logging.info("Start loading customers.csv into Dim Customers")

 df_customers = (
  pd.read_csv(
    BASE_DIR / "sources/customers.csv",
    dtype={
    "CustomerID" : "Int64",
    "customer_type": "string"}
   )
  .rename(columns={"CustomerID": "customer_id"})
 )

 if df_customers.empty:
  logging.warning("Customers table are empty")

 logging.info(f"Loading customers.csv is succeed, created DF is df_customer with len {len(df_customers)} rows")

except Exception as e:
 logging.critical(f"Failed to load customers.csv : {e}")
 raise ValueError()

try:

 logging.info("Start loading products.csv into Dim Products")

 df_products = (
  pd.read_csv(
   BASE_DIR / "sources/products.csv", 
   dtype={
    "product_id" : "Int64",
    "item": "string",
    "category": "string",
    "price": "float"}
   )
  )
 
 if df_products.empty:
  logging.warning("Products table are empty")

 logging.info(f"Loading products.csv is succeed, created DF is df_products with len {len(df_products)} rows")

except Exception as e:
 logging.critical(f"Failed to load products.csv : {e}")
 raise ValueError()

try:

 logging.info("Start loading purchases.csv into Src Purchases")

 df_purchases = (
  pd.read_csv(
   BASE_DIR / "sources/purchases.csv",
   dtype={
    "InvoiceID" : "Int64",
    "product_id": "Int64",
    "CustomerID" : "Int64",
    "quantity": "Int64"},
    parse_dates=["date"])
  .rename(columns={"InvoiceID" : "invoice_id", 
                     "date" : "invoice_date", 
                     "CustomerID" : "customer_id"})
 )

 if df_purchases.empty:
  logging.warning("Purchases table are empty")

 logging.info(f"Loading purchases.csv is succeed, created DF is df_purchases with len {len(df_purchases)} rows")

except Exception as e:
 logging.critical(f"Failed to load purchases.csv : {e}")
 raise ValueError()

try:

 logging.info("Start loading invoice_items.csv into Src Invoices")
 
 df_invoices = (
  pd.read_csv(
   BASE_DIR / "sources/invoice_items.csv",
   usecols = ["InvoiceID", "product_id", "quantity", "line_total"],
   dtype={
    "InvoiceID" : "Int64",
    "product_id": "Int64",
    "quantity": "Int64",
    "line_total": "float"})
  .rename(columns={"InvoiceID" : "invoice_id"})
 )

 if df_invoices.empty:
  logging.warning("Invoices table are empty")

 logging.info(f"Loading invoice_items.csv is succeed, created DF is df_invoices with len {len(df_purchases)} rows")

except Exception as e:
 logging.critical(f"Failed to load invoice_items.csv : {e}")
 raise ValueError()

try:

 logging.info("Start processing Src Purchases")

 df_purchases = df_purchases.groupby(
  ["invoice_id","product_id", "invoice_date", "customer_id"], as_index=False)["quantity"].sum()
 
 logging.info(f"Succeed processing Src Purchases, returned rows {len(df_purchases)}")

 logging.info("Start processing Src Invoices")

 df_invoices = (
  df_invoices.groupby(["invoice_id", "product_id"], as_index=False)
  .agg(
   quantity=("quantity", "sum"),
   line_total=("line_total", "sum")
   )
 )

 df_invoices["price"] = (df_invoices["line_total"]/df_invoices["quantity"]).astype("float")

 logging.info(f"Succeed processing Src Invoices, returned rows {len(df_invoices)}")

except Exception as e:
 logging.critical(f"Failed to process data: {e}")

#3.2 Обробка великих даних
"""
Завдання для себе я зрозумів наступним чином, якщо ми можемо завантажити файл через pd.read_, але на будь-які маніпуляції за цим DF - вже не вистачає пам'яті - що тоді робити?

Бо якщо, не вистачає пам'яті навіть завантажити через pd.read_ (без додаткових аргументів), то тут тількі підходить chunk, щоб розмір 1 chunk влазив в пам'ять. Бо ми не зможемо створити генератор на df, який не може створити.

Тому нам потрібно читати частинами
"""


""" pandas chunks
 В цьому випадку ми маємо chunksize - кількість рядків у 1 chunk. І працюємо в пам'яті лише з 1 chunk. df в цьому випадку вже не DataFrame, а iterator, який ще нічого не виконує/рахує.
 df = pd.read_csv("BigData_file.csv", chunksize = 1000000)

 Тільки коли ми почнемо ітерувати, то по факту будемо викликати next(iterator), повертати chunk, ставати на паузу, до поки не порахується сума, дойде до кінця поточного циклу, і далі буде повертати наступний chank, рахувати суму, і так до останнього chunk
 total_sum = 0
 for idx, chunk in enumerate(df):
  total_sum += chunk["quantity"].sum()

generators
 Логіка описана вище, але це імплементація генератора з застосування yield, де ми моделюэмо створення великого DF (просто 1000 копій одного й той самого DF), тобто ми можемо завантажити, але не обробити.


def example_gen(df, chunk_size):
 df_big = pd.concat([df] * 1000, ignore_index=True)
 df_big_len = len(df_big)
 index = 0
 while index < df_big_len:
  yield df_big.iloc[index:index+chunk_size]
  index += chunk_size

total_sum = 0
for idx, chunk in enumerate(example_gen(df = df_invoices, chunk_size = 1000000)):
 total_sum += chunk["quantity"].sum()

print(total_sum)
"""

logging.warning("Task 3.3 Data Quality Checks")
#3.3 Data Quality Checks

#A.перевірку на NULL значення

df_null_dict = {
 "customers" : df_customers,
 "products": df_products,
 "purchases": df_purchases,
 "invoice": df_invoices
}

for name, df in df_null_dict.items():
 df_null = df.isna().sum().reset_index()
 df_null.columns = ["column", "null_count"]
 df_null = df_null[df_null["null_count"] > 0]

 if not df_null.empty:
  logging.warning(f"\ndf_{name}_table contain next NUlls")
  logging.warning(df_null)
 logging.info(f"df_{name}_table does not conatain NULLs")

#B.пошук дублікатів
"""
Customers_dim_table:
 customer_id 1 <-> 1 customer_type або customer_id needs повинен бути унікальним
"""
df_customers_duplicates = (
 df_customers
 .groupby("customer_id")["customer_type"]
 .count()
 .reset_index(name="customer_type_count")
)

df_customers_duplicates = df_customers_duplicates[df_customers_duplicates["customer_type_count"] > 1]
if df_customers_duplicates.empty:
 logging.info(f"df_customers_table does not contain duplicates")
else:
 df_customers = df_customers.drop_duplicates(subset=["customer_id"],keep="last")
 logging.warning(f"df_customers_table contains duplicates, so it's prepared to keep last value of customer_id group")

"""
Products_dim_table:
 product_id 1 <-> 1 item or або product_id needs повинен бути унікальним
"""

is_df_products_unique = df_products["product_id"].is_unique

if is_df_products_unique:
 logging.info(f"df_products_table does not contain duplicates")
else:
 df_products = df_products.drop_duplicates(subset=["product_id"],keep="last")
 logging.warning(f"df_products_table contains duplicates, so it's prepared to keep last value of product_id group")

"""
Purchase_source_table:
 invoice_id, product_id: unique

Invoice_source_table:
 invoice_id, product_id: unique
"""

df_dupl_dict = {
 "purchases": df_purchases,
 "invoice": df_invoices
}
source_subset = ["invoice_id","product_id"]

for name, source in df_dupl_dict.items():
 has_duplicates = source.duplicated(subset=source_subset).any()
 if not has_duplicates:
  logging.info(f"df_{name}_table does not contain duplicates")
 else:
  source = source.drop_duplicates(subset = source_subset, keep = "last")
  logging.warning(f"df_{name}_table contains duplicates, so it's prepared to keep last value of subset {source_subset}")

#C.виявлення аномалій
"""
Наскількі я зрозумів, price в products.csv - базова ціна продажу, price в invoice.csv - це ціна у чеку по факту, яка може бути акційною
Тому перевіряю гіпотезу: 0 < invoice.csv <= ціна у products.csv
"""
df_price_anomaly = pd.merge(left = df_invoices, right = df_products, how = "left", on = "product_id", suffixes = ("_inv","_prd"))

df_price_anomaly["price_flag"] = np.where(
 (df_price_anomaly["price_inv"] > 0) &
 (df_price_anomaly["price_inv"] <= df_price_anomaly["price_prd"]),
 "valid",
 "invalid"
)

if (df_price_anomaly["price_flag"] == "invalid").any():
 logging.warning(
     "5 first invalid invoice price ASC\n"
     f"{df_price_anomaly.loc[
         df_price_anomaly['price_flag'] == 'invalid',
         ['price_inv','price_prd']
     ].sort_values(by='price_inv', ascending=True).head(5)}"
 )
 logging.warning(
     "5 last invalid invoice price DESC\n"
     f"{df_price_anomaly.loc[
         df_price_anomaly['price_flag'] == 'invalid',
         ['price_inv','price_prd']
     ].sort_values(by='price_inv', ascending=True).tail(5)}"
 )
else:
 logging.info("df_invoices_table does not contain anomaly on price")

df_invoices = df_price_anomaly.drop(columns=["item", "category", "price_prd"]).rename(columns={"price_inv":"price"})
df_invoices = df_invoices[["invoice_id","product_id", "quantity", "line_total", "price", "price_flag"]]

# product в моей моделі - dim
df_products = df_products.drop(columns=["price"])

#D.додаткові перевірки (Referential check)
#.D1 customers (customer_id) AND purchases (customer_id)

df_purch_cust_miss = df_purchases[
    ~df_purchases["customer_id"].isin(df_customers["customer_id"])
]
if df_purch_cust_miss.empty:
 logging.info("df_purchase_table passed the referencial check on df_customer_table on customer_id")
else:
 logging.warning(f"df_purchase_table does not pass the referencial check on df_customer_table on customer_id")

#.D2 product (product_id) AND purchases (prodocuct_id)
df_purch_prod_miss = df_purchases[
    ~df_purchases["product_id"].isin(df_products["product_id"])
]
if df_purch_prod_miss.empty:
 logging.info("df_purchase_table passed the referencial check on df_product_table on product_id")
else:
 logging.warning(f"df_purchase_table does not pass the referencial check on df_product_table on product_id")

#.D2 product (product_id) AND invoices (prodocuct_id)
df_inv_prod_miss = df_invoices[
    ~df_invoices["product_id"].isin(df_products["product_id"])
]
if df_inv_prod_miss.empty:
 logging.info("df_invoice_table passed the referencial check on df_product_table on product_id")
else:
 logging.warning(f"df_invoice_table does not pass the referencial check on df_product_table on product_id")


#3.4 Агрегації та аналітика
#розрахунок топ-N сутностей (топ 10 клієнтів за середнім чеком з 5+ покупками - df1)
logging.warning("Task 3.4: Агрегації та аналітика")

try:
 logging.info("Топ 10 клієнтів за середнім чеком з 5+ покупками")
 df1 = pd.merge(left = df_invoices, right = df_purchases, how = "left", on = ["invoice_id", "product_id"])
 df1 = df1.groupby("customer_id").agg(
  total_gross = ("line_total", "sum"),
  total_units = ("quantity_x", "sum"),
  invoice_count=("invoice_id", "nunique")
 ).reset_index()
 df1["avg_check"] = df1["total_gross"] / df1["invoice_count"]
 df1 = (df1.loc[
  (df1["total_units"] > 5) & (df1["total_units"] < 100)]
  .nlargest(10, columns="avg_check"))[["customer_id","avg_check"]]
 df1 = df1.merge(right = df_customers, how = "left", on = "customer_id")
 logging.info(f"\n{df1}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")


#агрегати за часом (Топ 5 клієнтів серед оптовиків з найбільшою кількістю покупок за грудень 2015 - df2)
try:
 logging.info("Топ 5 клієнтів серед оптовиків з найбільшою кількістю покупок за грудень 2015")
 df2_customers = df_customers.loc[df_customers["customer_type"] == "wholesaler"]
 df2_purchases = df_purchases.loc[(df_purchases["invoice_date"].dt.month == 12) & (df_purchases["invoice_date"].dt.year == 2015)]
 df2 = pd.merge(left = df2_customers, right = df2_purchases, how = "inner", on = "customer_id")
 df2 = df2.groupby("customer_id")["quantity"].sum().reset_index()
 df2 = df2.nlargest(5, columns="quantity")
 logging.info(f"\n{df2}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")

#розрахунок частки в межах групи (Частка кожного місяця у власному році - df3)
try:
 logging.info("Частка кожного місяця у власному році у розрізі Продажів UAH")
 df3 = pd.merge(left = df_purchases,right = df_invoices, how = "inner", on = ["invoice_id","product_id"])
 df3["year"] = df3["invoice_date"].dt.year
 df3["month"] = df3["invoice_date"].dt.month

 df3 = (
  df3.groupby([df3["year"], df3["month"]], 
   as_index=False)
   ["line_total"].sum()
 )

 df3["month_share"] = df3["line_total"] / df3.groupby("year")["line_total"].transform("sum")
 logging.info(f"\n{df3}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")

#накопичувальні метрики (Розрахувати накопичувальну кількість нових customers кожного місяця по кожному типу customers- df4)

try:
 logging.info("Накопичувальна кількість нових customers кожного місяця по кожному типу customers")
 df4 = pd.merge(left = df_purchases, right = df_customers, how = "left", on = "customer_id")
 df4["first_purchase_month"] = df4.groupby("customer_id")["invoice_date"].transform("min").dt.to_period("M")
 df4 = (
  df4.groupby(["customer_type","first_purchase_month"])
  .agg(new_customers=("customer_id", "nunique"))
  .reset_index())

 df4 = df4.sort_values(["customer_type", "first_purchase_month"])

 df4["new_customers_cum"] = df4.groupby(["customer_type"])["new_customers"].cumsum()
 logging.info(f"\n{df4}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")

#порівняння періодів (Порівняти LFL 2025 до 2024 по місяцям за продажами UAH по категорії Bakery & Snacks- df5)
try:
 logging.info("Приріст продажів UAH по категорії Bakery & Snacks 2015 року до 2014")
 df5_products = df_products.loc[df_products["category"] == "Bakery & Snacks"]
 df5 = pd.merge(left = df_purchases,right = df_invoices, how = "inner", on = ["invoice_id","product_id"])
 df5 = df5.merge(right = df5_products, how = "inner", on = "product_id")
 df5["year"] = df5["invoice_date"].dt.year
 df5["month"] = df5["invoice_date"].dt.month

 df5= df5.pivot_table(
  index=["month"],
  columns="year",
  values="line_total",
  aggfunc="sum"
 ).fillna(0)

 df5["LFL"] = df5[2015] / df5[2014] - 1
 logging.info(f"\n{df5}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")

#розрахунок перцентилів (Знайти такі товари, які в своїй категорії - в топ 10% продажах)
try:
 logging.info("Товари, які в своїй категорії - в топ 10% продажах")
 df6 = pd.merge(left = df_purchases,right = df_invoices, how = "inner", on = ["invoice_id","product_id"])
 df6 = df6.merge(right = df_products, how = "inner", on = "product_id")
 df6 = (
  df6.groupby(
   ["category","product_id"],
   as_index =False)
   ["line_total"].sum()
 )

 df6["perc_90"] = df6.groupby("category")["line_total"].transform(lambda x: x.quantile(0.9))

 df6= df6[df6["line_total"] >= df6["perc_90"]].sort_values(["category","line_total"], ascending=[True, False])
 logging.info(f"\n{df6}")
except Exception as e:
 logging.warning("Analytic ad-hoc failed : {e}")


logging.info("Start loading final table")
try:
 df_customers.to_csv("2.sql/sources/customers.csv", index=False)
 df_products.to_csv("2.sql/sources/products.csv", index=False)
 df_invoices.to_csv("2.sql/sources/invoices.csv", index=False)
 df_purchases.to_csv("2.sql/sources/purchases.csv", index=False)

except Exception as e:
 logging.error(f"Failed to save tables: {e}")
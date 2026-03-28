import pandas as pd
import awswrangler as wr
from datetime import datetime

data = {"transaction_id": "TXN20260326000516", "date": "2026-03-26", "time": "18:16:56", "datetime": "2026-03-26 18:16:56", "customer_id": "CUST032267", "store_id": "ST020", "store_name": "Short Group Express", "cashier_id": null, "payment_method": "Debit Card", "subtotal": 4273.84, "tax_amount": 341.91, "total_amount": 4615.74, "items_count": 4, "loyalty_points_earned": 461, "promotion_code": null, "refund_reason": null, "status": "Completed", "product_id": "PRD010705", "product_name": "Modern Hat", "category": "Clothing", "quantity": 1, "unit_price": 186.6517538904687, "discount_percent": 0, "line_total": 186.65}

date
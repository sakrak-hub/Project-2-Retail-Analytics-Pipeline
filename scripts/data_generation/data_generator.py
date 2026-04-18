import json
import os
import sys
from datetime import datetime

import pandas as pd

from store_generator import StoreGenerator
from product_generator import ProductGenerator
from customer_generator import CustomerGenerator
from transaction_generator import TransactionGenerator


class RetailDataGenerator:
    """Orchestrates all retail data generators and handles persistence."""

    def __init__(self, seed=42, add_noise=True, folder_path='/opt/airflow/master_data'):
        print("Initializing Enhanced Retail Data Generator with Realistic Quality Issues...")

        shared = dict(seed=seed, add_noise=add_noise)
        self.add_noise = add_noise

        self._store_gen = StoreGenerator(**shared)
        self._product_gen = ProductGenerator(num_products=12000, **shared)
        self._customer_gen = CustomerGenerator(num_customers=55000, **shared)

        # Expose noise_config from one of the generators for reporting
        self.noise_config = self._store_gen.noise_config

        # Master data
        self.stores: list = []
        self.products: list = []
        self.customers: list = []

        self.duplicate_customers: list = []
        self.duplicate_products: list = []
        self.duplicate_transactions: list = []

        self._load_or_generate_master_data(output_dir=folder_path)

    # ── Master data lifecycle ─────────────────────────────────────────────────

    def _load_or_generate_master_data(self, output_dir='/opt/airflow/master_data'):
        stores_file = f'{output_dir}/stores.parquet'
        products_file = f'{output_dir}/products.parquet'
        customers_file = f'{output_dir}/customers.parquet'

        if (os.path.exists(stores_file) and
                os.path.exists(products_file) and
                os.path.exists(customers_file)):
            print("Loading existing master data...")
            try:
                self.stores = pd.read_parquet(stores_file).to_dict('records')
                self.products = pd.read_parquet(products_file).to_dict('records')
                self.customers = pd.read_parquet(customers_file).to_dict('records')
                print(f"Loaded {len(self.stores)} stores, {len(self.products)} products, "
                      f"{len(self.customers)} customers")
                return
            except Exception as e:
                print(f"Error loading master data: {e}")
                print("Generating new master data...")

        print("Generating new master data...")
        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        self.save_master_data(output_dir)

    def _generate_stores(self):
        self.stores = self._store_gen.generate()

    def _generate_products(self):
        self.products, self.duplicate_products = self._product_gen.generate()

    def _generate_customers(self):
        self.customers, self.duplicate_customers = self._customer_gen.generate()

    def save_master_data(self, output_dir='/opt/airflow/master_data'):
        """Save master data (stores, products, customers) to Parquet files."""
        os.makedirs(output_dir, exist_ok=True)

        if self.stores:
            pd.DataFrame(self.stores).to_parquet(f'{output_dir}/stores.parquet', index=False)
        if self.products:
            pd.DataFrame(self.products).to_parquet(f'{output_dir}/products.parquet', index=False)
        if self.customers:
            pd.DataFrame(self.customers).to_parquet(f'{output_dir}/customers.parquet', index=False)

        quality_report = {
            'duplicate_customers': len(self.duplicate_customers),
            'duplicate_products': len(self.duplicate_products),
            'total_stores': len(self.stores),
            'total_products': len(self.products),
            'total_customers': len(self.customers),
            'noise_config': self.noise_config,
            'generation_timestamp': datetime.now().isoformat(),
        }
        with open(f'{output_dir}/data_quality_report.json', 'w') as f:
            json.dump(quality_report, f, indent=2)

        print(f"Master data saved to {output_dir}/")
        print(f"Generated {len(self.stores)} stores, {len(self.products)} products, "
              f"{len(self.customers)} customers")
        print(f"Data quality issues: {len(self.duplicate_customers)} duplicate customers, "
              f"{len(self.duplicate_products)} duplicate products")

    def force_regenerate_master_data(self, output_dir='/opt/airflow/master_data'):
        """Force regeneration of all master data."""
        print("Force regenerating master data...")
        self.stores = []
        self.products = []
        self.customers = []
        self.duplicate_customers = []
        self.duplicate_products = []

        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        self.save_master_data(output_dir)

    # ── Transaction generation ────────────────────────────────────────────────

    def _make_transaction_generator(self):
        return TransactionGenerator(
            products=self.products,
            customers=self.customers,
            stores=self.stores,
            target_monthly_transactions=120000,
            seed=42,
            add_noise=self.add_noise,
        )

    def generate_daily_transactions(self, date: datetime):
        """Generate transactions for a specific date."""
        gen = self._make_transaction_generator()
        transactions = gen.generate_daily(date)
        self.duplicate_transactions = gen.duplicate_transactions
        return transactions

    def generate_and_save_daily_data(self, date: datetime,
                                     output_dir='/tmp/retail_data/transactions'):
        """Generate and persist transaction data for a specific date."""
        os.makedirs(output_dir, exist_ok=True)

        date_str = date.strftime('%Y-%m-%d')
        transactions_file = f'{output_dir}/transactions_{date_str}.parquet'

        if os.path.exists(transactions_file):
            print(f"Transaction data for {date_str} already exists. Skipping generation.")
            print(f"To regenerate, delete {transactions_file} first.")
            return []

        transactions = self.generate_daily_transactions(date)
        gen = self._make_transaction_generator()  # for analytics helpers

        # Flatten nested items for Parquet
        flattened = []
        for txn in transactions:
            base = {k: v for k, v in txn.items() if k != 'items'}
            if txn['items']:
                for item in txn['items']:
                    flattened.append({**base, **item})
            else:
                base.update({
                    'product_id': None, 'product_name': None, 'category': None,
                    'quantity': 0, 'unit_price': 0, 'discount_percent': 0, 'line_total': 0,
                })
                flattened.append(base)

        if flattened:
            df = pd.DataFrame(flattened)
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.to_parquet(transactions_file, index=False)

        summary = {
            'date': date_str,
            'total_transactions': len(transactions),
            'total_revenue': sum(t['total_amount'] for t in transactions
                                 if isinstance(t['total_amount'], (int, float))),
            'total_items_sold': sum(t['items_count'] for t in transactions
                                    if isinstance(t['items_count'], (int, float))),
            'unique_customers': len(set(t['customer_id'] for t in transactions if t['customer_id'])),
            'duplicate_transactions': len(self.duplicate_transactions),
            'failed_transactions': len([t for t in transactions if t['status'] == 'Failed']),
            'refunded_transactions': len([t for t in transactions if t['status'] == 'Refunded']),
            'missing_timestamps': len([t for t in transactions if not t['datetime']]),
            'missing_cashier_ids': len([t for t in transactions if not t['cashier_id']]),
            'negative_quantities': len([t for t in transactions
                                        for item in t['items'] if item['quantity'] < 0]),
            'payment_method_breakdown': gen.get_payment_breakdown(transactions),
            'category_breakdown': gen.get_category_breakdown(transactions),
            'top_products': gen.get_top_products(transactions),
        }

        with open(f'{output_dir}/daily_summary_{date_str}.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"Generated {len(transactions)} transactions for {date_str}")
        print(f"Total revenue: ${summary['total_revenue']:,.2f}")
        print(f"Data quality issues: {summary['duplicate_transactions']} duplicates, "
              f"{summary['failed_transactions']} failed, "
              f"{summary['missing_timestamps']} missing timestamps")
        print(f"Files saved: {transactions_file}")

        return transactions

    # ── Legacy analytics helpers (kept for backward compatibility) ────────────

    def _get_payment_breakdown(self, transactions):
        return self._make_transaction_generator().get_payment_breakdown(transactions)

    def _get_category_breakdown(self, transactions):
        return self._make_transaction_generator().get_category_breakdown(transactions)

    def _get_top_products(self, transactions, top_n=10):
        return self._make_transaction_generator().get_top_products(transactions, top_n)


def generate_transactions(year, month, date):
    """Entry point: generate transactions for a given date."""
    print("=" * 70)
    generator = RetailDataGenerator(add_noise=True)

    print(f"\nGenerating transaction data for {year}-{month}-{date}...")
    data_date = datetime(int(year), int(month), int(date))
    generator.generate_and_save_daily_data(data_date)

    print("\nData generation complete!")
    print("=" * 70)


if __name__ == "__main__":
    generate_transactions(sys.argv[1], sys.argv[2], sys.argv[3])

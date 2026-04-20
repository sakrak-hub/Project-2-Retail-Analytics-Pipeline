import random
from datetime import datetime, timedelta
from typing import Dict, List
from base_generator import BaseGenerator
from variance_pool import VariancePool


class TransactionGenerator(BaseGenerator):


    def __init__(self, products: list, customers: list, stores: list,
                 target_monthly_transactions=120000, **kwargs):
        super().__init__(**kwargs)
        self.products = products
        self.customers = customers
        self.stores = stores
        self.daily_transactions = target_monthly_transactions // 30
        self.duplicate_transactions = []
        
        # ← NEW: Create variance pool for realistic daily variance
        seed = kwargs.get('seed', 42)
        self.variance_pool = VariancePool(seed=seed)
 
    def generate_daily(self, date: datetime) -> List[Dict]:
        """Generate transactions for a specific date with realistic variance."""
        self.duplicate_transactions = []
        transactions = []
 
        # BASE: Day-of-week pattern (weekends busier)
        day_multipliers = {
            0: 1.0,   # Monday
            1: 0.95,  # Tuesday
            2: 0.98,  # Wednesday
            3: 1.05,  # Thursday
            4: 1.25,  # Friday
            5: 1.45,  # Saturday
            6: 1.15,  # Sunday
        }
        base_multiplier = day_multipliers[date.weekday()]
        
        daily_variance = self.variance_pool.get_daily_multiplier(date)
        
        # ADD SEASONALITY: Monthly trends
        month_multipliers = {
            1: 0.85,   # January (post-holiday slowdown)
            2: 0.90,   # February
            3: 0.95,   # March
            4: 1.00,   # April
            5: 1.05,   # May
            6: 1.10,   # June
            7: 1.08,   # July
            8: 1.03,   # August
            9: 1.00,   # September
            10: 1.10,  # October
            11: 1.25,  # November (Black Friday)
            12: 1.40,  # December (Holiday season)
        }
        seasonal_multiplier = month_multipliers[date.month]
        
        # ADD EVENTS: 10% chance of extra boost
        event_multiplier = 1.0
        if random.random() < 0.10:
            event_multiplier = random.uniform(1.3, 1.5)
        
        # COMBINE ALL FACTORS
        total_multiplier = (
            base_multiplier * 
            daily_variance *     
            seasonal_multiplier * 
            event_multiplier
        )
        
        daily_volume = int(self.daily_transactions * total_multiplier)
        
        # Add some noise
        noise = int(random.gauss(0, daily_volume * 0.1))
        daily_volume = daily_volume + noise
        daily_volume = max(100, daily_volume)
 
        for i in range(daily_volume):
            store_choice = len(self.stores)
            while store_choice>(len(self.stores)-1):
                store_choice = abs(int(random.gauss(len(self.stores)//2,len(self.stores)*daily_variance)))
            customer = random.choice(self.customers)
            store = self.stores[store_choice]
            transaction = self._generate_single_transaction(date, customer, store, i + 1)
            transactions.append(transaction)
 
        # Add duplicate transactions (15% duplicate rate)
        if self.add_noise:
            num_duplicates = int(len(transactions) * self.noise_config['duplicate_transactions'])
            for _ in range(num_duplicates):
                original = random.choice(transactions)
                duplicate = original.copy()
 
                duplicate['transaction_id'] = f"DUP{duplicate['transaction_id']}"
 
                if duplicate['datetime']:
                    original_dt = datetime.strptime(duplicate['datetime'], '%Y-%m-%d %H:%M:%S')
                    new_dt = original_dt + timedelta(minutes=random.randint(1, 30))
                    duplicate['datetime'] = new_dt.strftime('%Y-%m-%d %H:%M:%S')
                    duplicate['time'] = new_dt.strftime('%H:%M:%S')
 
                if random.random() < 0.3:
                    duplicate['status'] = 'Failed'
 
                transactions.append(duplicate)
                self.duplicate_transactions.append((original['transaction_id'], duplicate['transaction_id']))
 
        return transactions

    def _generate_single_transaction(self, date: datetime, customer: Dict,
                                     store: Dict, transaction_num: int) -> Dict:
        """Generate a single transaction with realistic data quality issues."""
        base_transaction_time = date.replace(
            hour=random.randint(8, 22),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        transaction_time = self._introduce_timestamp_issues(base_transaction_time)

        if self.add_noise and random.random() < self.noise_config['missing_transaction_time']:
            transaction_time = None

        status = 'Completed'
        refund_reason = None

        if self.add_noise:
            if random.random() < 0.015:
                status = 'Failed'
            elif random.random() < 0.025:
                status = 'Refunded'
                refund_reason = random.choice([
                    'Customer request', 'Defective product', 'Wrong item',
                    'Price adjustment', 'Damaged packaging', 'Changed mind',
                ])

        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 7, 3])[0]
        available_products = self.products.copy()

        # Reference integrity errors
        if self.add_noise and random.random() < self.noise_config['reference_integrity_errors']:
            fake_product = {
                'product_id': f'INVALID{random.randint(1000, 9999)}',
                'product_name': 'Unknown Product',
                'category': 'Unknown',
                'price': 0.00,
            }
            available_products.append(fake_product)

        selected_products = random.sample(available_products, min(num_items, len(available_products)))

        subtotal = 0
        items = []

        for product in selected_products:
            quantity = random.randint(1, 3)
            if self.add_noise and random.random() < self.noise_config['negative_quantities']:
                quantity = random.randint(-5, -1)

            # PRICE VARIANCE: Use NORMAL distribution (not uniform!)
            unit_price = product.get('price', 0)
            if isinstance(unit_price, str):
                try:
                    unit_price = float(unit_price.replace('$', '').replace(',', ''))
                except Exception:
                    unit_price = 0.00
            
            # Use gauss for price variance too
            price_variance = random.gauss(1.0, 0.1)  # ±10% typically
            price_variance = max(0.8, min(1.3, price_variance))  # Clamp extremes
            unit_price = unit_price * price_variance

            discount = 0
            if random.random() < 0.15:
                discount = random.uniform(0.05, 0.25)

            discounted_price = unit_price * (1 - discount)
            line_total = discounted_price * quantity
            subtotal += line_total

            items.append({
                'product_id': product['product_id'],
                'product_name': self._introduce_encoding_issues(product.get('product_name', 'Unknown')),
                'category': product.get('category', 'Unknown'),
                'quantity': quantity,
                'unit_price': round(unit_price, 2),  # Save the varied price
                'discount_percent': round(discount * 100, 2),
                'line_total': round(line_total, 2),
            })

        # Tax variance using gauss
        tax_rate = random.gauss(0.08, 0.01)  # Mean 8%, std 1%
        tax_rate = max(0.06, min(0.10, tax_rate))  # 6-10% range
        tax_amount = subtotal * tax_rate
        total_amount = subtotal + tax_amount

        if self.add_noise and random.random() < 0.05:
            tax_amount = subtotal * random.uniform(0.05, 0.15)
            total_amount = subtotal + tax_amount

        cashier_id = f'EMP{random.randint(1, 200):03d}'
        if self.add_noise and random.random() < self.noise_config['missing_cashier_id']:
            cashier_id = None

        payment_method = self._weighted_choice(self.payment_methods)
        if self.add_noise and random.random() < self.noise_config['missing_payment_method']:
            payment_method = None

        return {
            'transaction_id': f'TXN{date.strftime("%Y%m%d")}{transaction_num:06d}',
            'date': date.strftime('%Y-%m-%d') if transaction_time else None,
            'time': transaction_time.strftime('%H:%M:%S') if transaction_time else None,
            'datetime': transaction_time.strftime('%Y-%m-%d %H:%M:%S') if transaction_time else None,
            'customer_id': customer['customer_id'],
            'store_id': store['store_id'],
            'store_name': self._introduce_encoding_issues(store.get('store_name', '')),
            'cashier_id': cashier_id,
            'payment_method': payment_method,
            'subtotal': round(subtotal, 2),
            'tax_amount': round(tax_amount, 2),
            'total_amount': round(total_amount, 2),
            'items_count': num_items,
            'items': items,
            'loyalty_points_earned': int(total_amount * 0.1) if customer.get('loyalty_member') else 0,
            'promotion_code': self._generate_promotion_code() if random.random() < 0.1 else None,
            'refund_reason': refund_reason,
            'status': status,
        }

    def _generate_promotion_code(self) -> str:
        """Generate a random promotion code."""
        codes = ['SAVE10', 'SUMMER20', 'NEWCUST15', 'LOYALTY5', 'WEEKEND25', 'FLASH30']
        return random.choice(codes)

    # ── Analytics helpers ─────────────────────────────────────────────────────
    # These methods are called by data_generator.py - DO NOT REMOVE!

    def get_payment_breakdown(self, transactions: List[Dict]) -> Dict:
        """Return payment-method counts for a list of transactions."""
        payment_counts = {}
        for txn in transactions:
            method = txn['payment_method'] or 'Unknown'
            payment_counts[method] = payment_counts.get(method, 0) + 1
        return payment_counts

    def get_category_breakdown(self, transactions: List[Dict]) -> Dict:
        """Return category sales breakdown for a list of transactions."""
        category_sales = {}
        for txn in transactions:
            for item in txn['items']:
                category = item['category'] or 'Unknown'
                if category not in category_sales:
                    category_sales[category] = {'count': 0, 'revenue': 0}
                category_sales[category]['count'] += abs(item['quantity'])
                category_sales[category]['revenue'] += item['line_total']
        return category_sales

    def get_top_products(self, transactions: List[Dict], top_n: int = 10) -> Dict:
        """Return the top-N products by revenue."""
        product_sales = {}
        for txn in transactions:
            for item in txn['items']:
                pid = item['product_id']
                if pid not in product_sales:
                    product_sales[pid] = {
                        'product_name': item['product_name'],
                        'quantity_sold': 0,
                        'revenue': 0,
                    }
                product_sales[pid]['quantity_sold'] += abs(item['quantity'])
                product_sales[pid]['revenue'] += item['line_total']

        sorted_products = sorted(product_sales.items(), key=lambda x: x[1]['revenue'], reverse=True)
        return dict(sorted_products[:top_n])

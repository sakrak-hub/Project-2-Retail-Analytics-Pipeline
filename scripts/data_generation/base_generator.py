import random
from faker import Faker


class BaseGenerator:
    """Shared configuration and utility methods for all generator classes."""

    def __init__(self, seed=42, add_noise=True):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        self.add_noise = add_noise

        self.noise_config = {
            # Missing Data Issues
            'missing_customer_email': 0.35,
            'missing_customer_phone': 0.40,
            'missing_customer_address': 0.25,
            'missing_product_description': 0.30,
            'missing_product_sku': 0.15,
            'missing_cashier_id': 0.20,
            'missing_payment_method': 0.10,
            'missing_transaction_time': 0.05,

            # Duplicate Issues
            'duplicate_customers': 0.30,
            'duplicate_transactions': 0.15,
            'duplicate_products': 0.25,
            'duplicate_stores': 0.20,

            # Data Quality Issues
            'invalid_email_format': 0.12,
            'inconsistent_phone_format': 0.25,
            'price_inconsistency': 0.08,
            'negative_quantities': 0.03,
            'extreme_prices': 0.05,
            'future_dates': 0.02,
            'encoding_issues': 0.08,
            'data_type_inconsistency': 0.15,
            'reference_integrity_errors': 0.10,
        }

        self.product_categories = {
            'Electronics': (50, 2000),
            'Clothing': (15, 300),
            'Home & Garden': (10, 500),
            'Books': (8, 60),
            'Sports & Outdoors': (20, 800),
            'Beauty & Personal Care': (5, 150),
            'Food & Beverages': (2, 50),
            'Toys & Games': (10, 200),
            'Automotive': (15, 1500),
            'Health & Wellness': (10, 300),
            'Office Supplies': (3, 200),
            'Pet Supplies': (5, 100),
        }

        self.payment_methods = {
            'Credit Card': 0.45,
            'Debit Card': 0.25,
            'Cash': 0.15,
            'Digital Wallet': 0.10,
            'Gift Card': 0.05,
        }

    def _introduce_encoding_issues(self, text):
        """Introduce realistic encoding and special character issues."""
        if not text or not self.add_noise or random.random() > self.noise_config['encoding_issues']:
            return text

        issues = [
            lambda s: s.replace('a', 'ä').replace('o', 'ö').replace('u', 'ü'),
            lambda s: s.replace(' ', '\u00A0'),
            lambda s: s + '\u200B',
            lambda s: s.replace("'", "\u2019"),
            lambda s: s.replace('"', '\u201C'),
            lambda s: s.encode('latin1', errors='ignore').decode('latin1', errors='ignore'),
            lambda s: s + '�',  # Replacement character (U+FFFD)
        ]

        return random.choice(issues)(text)

    def _introduce_timestamp_issues(self, base_datetime):
        """Placeholder — timestamp noise is currently disabled."""
        return base_datetime

    def _weighted_choice(self, choices_dict):
        """Make a weighted random choice from a dictionary."""
        choices = list(choices_dict.keys())
        weights = list(choices_dict.values())
        return random.choices(choices, weights=weights)[0]

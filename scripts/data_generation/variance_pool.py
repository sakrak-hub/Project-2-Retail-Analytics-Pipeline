import random
from typing import List


class VariancePool:
   
    def __init__(self, seed=None, pool_size=49, min_mult=0.2, max_mult=1.5):

        self.seed = seed
        self.pool_size = pool_size
        self.min_mult = min_mult
        self.max_mult = max_mult
        
        # Set seed if provided
        if seed is not None:
            random.seed(seed)
        
        # Generate the pool
        self.pool = self._create_pool()
    
    def _create_pool(self) -> List[float]:
        """
        Create a pool of unevenly distributed multipliers.
        
        Distribution (for default 49 numbers):
        - 0.6-0.8 (slow days):      7 numbers  (14%)
        - 0.8-1.0 (below average): 14 numbers  (29%)
        - 1.0-1.2 (above average): 21 numbers  (43%)  ← Most common
        - 1.2-1.4 (busy days):      7 numbers  (14%)
        
        Returns:
            List of 49 floating-point multipliers
        """
        random.seed()
        li = random.sample(range(1,21),3)
        random.shuffle(li)
        dist_list=[]
        for num in li:
            dist_list.append(random.randint(num,49))
            num-=dist_list[-1]
        dist_list.append(49-sum(dist_list))
        slow_day,below_avg,above_avg,busy = tuple(dist_list)
        pool = []
        
        # Calculate range and bin sizes
        range_span = self.max_mult - self.min_mult  # 0.8
        quarter = range_span / 4  # 0.2
        
        # SLOW DAYS: Bottom 25% of range (14% of numbers)
        slow_count = max(1, self.pool_size // slow_day)
        for _ in range(slow_count):
            pool.append(random.uniform(
                self.min_mult, 
                self.min_mult + quarter
            ))
        
        # BELOW AVERAGE: 25-50% of range (29% of numbers)
        below_count = max(1, self.pool_size * below_avg // 49)
        for _ in range(below_count):
            pool.append(random.uniform(
                self.min_mult + quarter,
                self.min_mult + 2 * quarter
            ))
        
        # ABOVE AVERAGE: 50-75% of range (43% of numbers) ← MOST COMMON
        above_count = max(1, self.pool_size * above_avg // 49)
        for _ in range(above_count):
            pool.append(random.uniform(
                self.min_mult + 2 * quarter,
                self.min_mult + 3 * quarter
            ))
        
        # BUSY DAYS: Top 25% of range (14% of numbers)
        busy_count = self.pool_size - (slow_count + below_count + above_count)
        for _ in range(busy_count):
            pool.append(random.uniform(
                self.min_mult + 3 * quarter,
                self.max_mult
            ))
        
        # Shuffle the entire pool
        random.shuffle(pool)
        
        return pool
    
    def get_weekly_multipliers(self, week_number: int, year: int = 2026) -> List[float]:
        """
        Get 7 multipliers for a specific week.
        
        Same week always returns same multipliers (deterministic per week).
        Different weeks return different multipliers (variance week-to-week).
        
        Args:
            week_number: ISO week number (1-52)
            year: Year (for seed generation)
        
        Returns:
            List of 7 multipliers for [Mon, Tue, Wed, Thu, Fri, Sat, Sun]
        """
        # Create a deterministic seed based on week and year
        week_seed = week_number * 1000 + year
        
        # Save current random state
        current_state = random.getstate()
        
        # Temporarily set seed for this week
        random.seed(week_seed)
        
        # Sample 7 numbers from the pool
        weekly_sample = random.sample(self.pool, 7)
        
        # Scramble them
        random.shuffle(weekly_sample)
        
        # Restore original random state
        random.setstate(current_state)
        
        return weekly_sample
    
    def get_daily_multiplier(self, date) -> float:
        """
        Get the multiplier for a specific date.
        
        Args:
            date: datetime object or date with isocalendar() method
        
        Returns:
            Float multiplier for that specific day
        """
        # Get ISO week number and year
        iso_calendar = date.isocalendar()
        week_number = iso_calendar[1]
        year = iso_calendar[0]
        day_of_week = date.weekday()  # 0=Monday, 6=Sunday
        
        # Get the weekly multipliers
        weekly_mults = self.get_weekly_multipliers(week_number, year)
        
        # Return this day's multiplier
        return weekly_mults[day_of_week]
    
    def get_pool_stats(self) -> dict:
        """
        Get statistics about the multiplier pool.
        
        Returns:
            Dictionary with pool statistics
        """
        import statistics
        
        sorted_pool = sorted(self.pool)
        
        # Calculate distribution bins
        range_span = self.max_mult - self.min_mult
        quarter = range_span / 4
        
        bins = {
            f'{self.min_mult:.1f}-{self.min_mult + quarter:.1f}': 
                len([x for x in sorted_pool if self.min_mult <= x < self.min_mult + quarter]),
            f'{self.min_mult + quarter:.1f}-{self.min_mult + 2*quarter:.1f}': 
                len([x for x in sorted_pool if self.min_mult + quarter <= x < self.min_mult + 2*quarter]),
            f'{self.min_mult + 2*quarter:.1f}-{self.min_mult + 3*quarter:.1f}': 
                len([x for x in sorted_pool if self.min_mult + 2*quarter <= x < self.min_mult + 3*quarter]),
            f'{self.min_mult + 3*quarter:.1f}-{self.max_mult:.1f}': 
                len([x for x in sorted_pool if self.min_mult + 3*quarter <= x <= self.max_mult]),
        }
        
        return {
            'pool_size': len(self.pool),
            'min': min(sorted_pool),
            'max': max(sorted_pool),
            'mean': statistics.mean(sorted_pool),
            'median': statistics.median(sorted_pool),
            'stdev': statistics.stdev(sorted_pool),
            'distribution': bins,
        }
    
    def regenerate_pool(self, seed=None):
        """
        Regenerate the pool with a new seed.
        
        Args:
            seed: New random seed (None = random)
        """
        if seed is not None:
            random.seed(seed)
        
        self.pool = self._create_pool()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_variance_pool(seed=42, pool_size=49, min_mult=0.6, max_mult=1.4):
    """
    Convenience function to create a variance pool.
    
    Args:
        seed: Random seed for reproducibility
        pool_size: Number of multipliers in pool
        min_mult: Minimum multiplier value
        max_mult: Maximum multiplier value
    
    Returns:
        VariancePool instance
    """
    return VariancePool(seed=seed, pool_size=pool_size, min_mult=min_mult, max_mult=max_mult)


def get_weekly_multipliers(week_number, year=2026, seed=42):
    """
    Quick function to get weekly multipliers without creating a pool instance.
    
    Args:
        week_number: ISO week number (1-52)
        year: Year
        seed: Random seed
    
    Returns:
        List of 7 multipliers for the week
    """
    pool = VariancePool(seed=seed)
    return pool.get_weekly_multipliers(week_number, year)


# ============================================================================
# DEMONSTRATION
# ============================================================================

if __name__ == "__main__":
    print("="*80)
    print("VARIANCE POOL MODULE - DEMONSTRATION")
    print("="*80)
    
    # Create a pool
    pool = VariancePool(seed=42)
    
    # Show pool statistics
    print("\n📊 POOL STATISTICS:")
    print("-" * 80)
    stats = pool.get_pool_stats()
    
    print(f"Pool size: {stats['pool_size']}")
    print(f"Range: {stats['min']:.3f} to {stats['max']:.3f}")
    print(f"Mean: {stats['mean']:.3f}")
    print(f"Median: {stats['median']:.3f}")
    print(f"Std Dev: {stats['stdev']:.3f}")
    
    print("\nDistribution:")
    for label, count in stats['distribution'].items():
        pct = (count / stats['pool_size']) * 100
        bar = '█' * int(pct / 2)
        print(f"  {label:<12} {count:2} ({pct:5.1f}%) {bar}")
    
    # Show weekly samples
    print("\n📅 WEEKLY MULTIPLIERS (4 weeks):")
    print("-" * 80)
    
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for week in range(1, 5):
        print(f"\nWeek {week}:")
        weekly_mults = pool.get_weekly_multipliers(week_number=week, year=2026)
        
        for day, mult in zip(days, weekly_mults):
            print(f"  {day}: {mult:.3f}")
    
    # Show daily multiplier usage
    print("\n📆 DAILY MULTIPLIER USAGE:")
    print("-" * 80)
    
    from datetime import datetime, timedelta
    
    start_date = datetime(2026, 1, 5)  # Monday, Week 2
    
    for i in range(7):
        date = start_date + timedelta(days=i)
        mult = pool.get_daily_multiplier(date)
        day_name = date.strftime('%A')
        week_num = date.isocalendar()[1]
        
        print(f"{date.strftime('%Y-%m-%d')} ({day_name}, Week {week_num}): {mult:.3f}")
    
    # Variance demonstration
    print("\n🔍 VARIANCE DEMONSTRATION (All Mondays in January):")
    print("-" * 80)
    
    monday_multipliers = []
    date = datetime(2026, 1, 5)  # First Monday
    
    print(f"{'Date':<12} {'Week':<6} {'Multiplier':<12} {'Revenue @ $4M base'}")
    print("-" * 60)
    
    for i in range(4):
        mult = pool.get_daily_multiplier(date)
        monday_multipliers.append(mult)
        week_num = date.isocalendar()[1]
        revenue = 4_000_000 * mult
        
        print(f"{date.strftime('%Y-%m-%d'):<12} {week_num:<6} {mult:<12.3f} ${revenue:,.0f}")
        
        date += timedelta(days=7)
    
    import statistics
    
    if len(monday_multipliers) > 1:
        avg = statistics.mean(monday_multipliers)
        std = statistics.stdev(monday_multipliers)
        cv = (std / avg) * 100
        
        print("-" * 60)
        print(f"Average: {avg:.3f}")
        print(f"Std Dev: {std:.3f}")
        print(f"CV: {cv:.1f}%")

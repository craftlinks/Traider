import argparse
import pprint
from typing import List
import asyncio
from datetime import date
import traider.yfinance as yf
from traider.yfinance._models import EarningsEvent


async def main() -> None:
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=lambda s: date.fromisoformat(s), default=date(2025, 8, 28))
    args = parser.parse_args()
    
    
    earnings: List[EarningsEvent] = await yf.get_earnings(args.start_date)
    pprint.pprint(earnings)

    for e in earnings:
        await e.to_db()


if __name__ == "__main__":
    asyncio.run(main())
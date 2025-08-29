import asyncio
from datetime import date
import traider.yfinance as yf


async def main() -> None:
    df = await yf.get_earnings(date(2025, 8, 28))
    print(df)


if __name__ == "__main__":
    asyncio.run(main())
"""platforms package.

Intentionally empty to avoid import-time coupling between orthogonal platform
layers. Import concrete implementations directly from their submodules, e.g.:

    from traider.platforms.brokers.interactive_brokers import InteractiveBrokersPlatform
    from traider.platforms.market_data.alpaca import AlpacaMarketData
"""

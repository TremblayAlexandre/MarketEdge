import json

def lambda_handler(event, context):
    """
    Handler for /api/lookup
    Returns a dummy mock up of lookup
    """

    companies = [
        {
            "Ticker": "AAPL",
            "PredictedPosition": 0.72,
            "reasoning": "Strong Q4 earnings, resilient iPhone demand in China despite supply chain pressures, and growing AI integration in devices.",
            "confidence_level": 0.61
        },
        {
            "Ticker": "TSLA",
            "PredictedPosition": 0.35,
            "reasoning": "Positive long-term growth in energy storage and AI-driven robotics, but short-term margins under pressure from EV price wars.",
            "confidence_level": 0.47
        },
        {
            "Ticker": "AMZN",
            "PredictedPosition": 0.58,
            "reasoning": "AWS showing strong rebound in enterprise demand; consumer spending stable despite macro uncertainty.",
            "confidence_level": 0.55
        },
        {
            "Ticker": "NVDA",
            "PredictedPosition": 0.89,
            "reasoning": "Dominant position in AI chip market, continued data center growth, and strong ecosystem lock-in among hyperscalers.",
            "confidence_level": 0.73
        },
        {
            "Ticker": "META",
            "PredictedPosition": 0.44,
            "reasoning": "Ad revenue stabilization, promising AI model integration, but facing regulatory and metaverse ROI concerns.",
            "confidence_level": 0.49
        },
        {
            "Ticker": "XOM",
            "PredictedPosition": -0.22,
            "reasoning": "Energy transition policies expected to impact long-term profitability; oil price volatility poses short-term uncertainty.",
            "confidence_level": 0.58
        },
        {
            "Ticker": "NFLX",
            "PredictedPosition": 0.27,
            "reasoning": "Content strategy and global subscriber growth steady, but valuation stretched and competition intensifying.",
            "confidence_level": 0.42
        },
        {
            "Ticker": "GOOGL",
            "PredictedPosition": 0.64,
            "reasoning": "Search and cloud divisions performing well; Gemini and AI infrastructure providing competitive edge.",
            "confidence_level": 0.68
        },
        {
            "Ticker": "MSFT",
            "PredictedPosition": 0.79,
            "reasoning": "Azure growth accelerating due to AI workloads, strong enterprise positioning, and diversification across sectors.",
            "confidence_level": 0.74
        },
        {
            "Ticker": "COIN",
            "PredictedPosition": -0.41,
            "reasoning": "Crypto market recovery uncertain; high regulatory risk in the U.S. and dependency on trading volume.",
            "confidence_level": 0.52
        }
    ]

    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "companies": companies
        })
    }

    return response

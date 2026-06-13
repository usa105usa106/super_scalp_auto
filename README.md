# MEXC Micro Maker LIVE Bot v0025

## v0025: Zero-fee guard / no false-positive scalping

v0023/v0024 showed the real problem: the bot can mark a 1-tick move as a virtual win while the MEXC account balance falls because the exact contract still charges real fees or execution is worse than the top-of-book estimate.

v0025 changes the live gate:

- Before every real entry, the bot queries `/api/v1/private/account/contract/fee_rate` for the exact symbol.
- If maker or taker fee is non-zero, the symbol is skipped and can be ignored for the session.
- Closed trade PnL is counted from real USDT equity delta when possible.
- Fee-aware target remains enabled as a second safety net.
- API messages stay in chat, log retention is still 20 minutes, ignored cache is cleared on restart, and panel time uses +3 hours.

Recommended after deploy:

```text
/preset plus
/ignore clear
```

Important: this version is intentionally stricter. If MEXC charges real fees on all API contracts for this account, the bot should refuse to scalp instead of producing many losing 1-tick trades.

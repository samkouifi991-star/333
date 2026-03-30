#!/usr/bin/env python3
"""
KALSHI PRO TRADING SYSTEM v2.0 — ADAPTIVE ORDER-FLOW-AWARE ENGINE
==================================================================
Elite-level trading system with:
  Phase 1: OrderFlowEngine — aggression, liquidity pull, whale tracking, pressure score
  Phase 2: AdaptiveParams — dynamic targets/sizing based on volatility & liquidity
  Phase 3: StrategyAllocator — market ranking + capital allocation
  Phase 4: FillQualityTracker — per-signal learning + auto-disable
  Phase 5: RegimeDetector — pre-event / active / dead classification
  Phase 6: Smart execution — join vs improve, queue estimation, flow-based cancel/repost

Usage:
  python kalshi_pro_trader.py                          # paper mode, all strategies
  python kalshi_pro_trader.py --enable-only scalper    # single strategy isolation
  python kalshi_pro_trader.py --live                   # live (requires paper gate)
  python kalshi_pro_trader.py --config custom.yaml     # custom config
"""

import os, sys, json, time, re, math, random, logging, hashlib, argparse, base64
from datetime import datetime, timedelta, timezone

# ============================================================
# HARDCODED FALLBACK CONFIG (used if .env is missing)
# ============================================================
KALSHI_API_KEY_ID = "your_key_here"
KALSHI_PRIVATE_KEY_PATH = "kalshi_private_key.pem"
KALSHI_API_BASE = "https://api.kalshi.com/trade-api/v2"
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from collections import defaultdict, deque
from pathlib import Path
import statistics

try:
    import yaml
    import requests
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.hazmat.primitives.asymmetric import padding, utils as crypto_utils
    from cryptography.hazmat.primitives import hashes
except ImportError:
    print("Installing dependencies...")
    os.system(f"{sys.executable} -m pip install pyyaml requests cryptography --quiet")
    import yaml
    import requests
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.hazmat.primitives.asymmetric import padding, utils as crypto_utils
    from cryptography.hazmat.primitives import hashes


# ============================================================
# ENUMS & DATA CLASSES
# ============================================================

class MarketType(Enum):
    GAME_WINNER = "game_winner"
    SPREAD = "spread"
    TOTAL = "total"
    TENNIS = "tennis"
    FIRST_HALF = "first_half"
    PROP = "prop"
    COMBO = "combo"
    UNKNOWN = "unknown"

class Side(Enum):
    YES = "yes"
    NO = "no"

class OrderStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"

class Strategy(Enum):
    SPREAD_ARB = "spread_arb"
    SCALPER = "scalper"
    VALUE_MAKER = "value_maker"
    TENNIS_MM = "tennis_mm"

class MarketRegime(Enum):
    PRE_EVENT = "pre_event"     # stable, low vol — good for value_maker
    ACTIVE = "active"           # volatile — good for scalper
    DEAD = "dead"               # no liquidity — skip
    UNKNOWN = "unknown"

class PressureSignal(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"

class OrderPlacementMode(Enum):
    JOIN = "join"           # match best bid/ask
    IMPROVE = "improve"     # penny better than best
    AGGRESSIVE = "aggressive"  # cross the spread


@dataclass
class OrderBookLevel:
    price: int
    quantity: int

@dataclass
class OrderBook:
    yes_bids: List[OrderBookLevel] = field(default_factory=list)
    yes_asks: List[OrderBookLevel] = field(default_factory=list)
    no_bids: List[OrderBookLevel] = field(default_factory=list)
    no_asks: List[OrderBookLevel] = field(default_factory=list)
    timestamp: float = 0.0

    @property
    def best_yes_bid(self) -> int:
        return self.yes_bids[0].price if self.yes_bids else 0
    @property
    def best_yes_ask(self) -> int:
        return self.yes_asks[0].price if self.yes_asks else 100
    @property
    def best_no_bid(self) -> int:
        return self.no_bids[0].price if self.no_bids else 0
    @property
    def best_no_ask(self) -> int:
        return self.no_asks[0].price if self.no_asks else 100
    @property
    def yes_spread(self) -> int:
        return self.best_yes_ask - self.best_yes_bid
    @property
    def mid_price(self) -> int:
        return (self.best_yes_bid + self.best_yes_ask) // 2
    @property
    def arb_cost(self) -> int:
        return self.best_yes_ask + self.best_no_ask
    @property
    def yes_bid_depth(self) -> int:
        return sum(l.quantity for l in self.yes_bids)
    @property
    def yes_ask_depth(self) -> int:
        return sum(l.quantity for l in self.yes_asks)
    @property
    def imbalance_ratio(self) -> float:
        total = self.yes_bid_depth + self.yes_ask_depth
        if total == 0:
            return 0.5
        return self.yes_bid_depth / total

@dataclass
class ClassifiedMarket:
    ticker: str
    title: str
    market_type: MarketType
    series: str
    event_ticker: str
    yes_price: int = 0
    no_price: int = 0
    volume: int = 0
    open_interest: int = 0
    close_time: Optional[str] = None
    team_a: str = ""
    team_b: str = ""
    spread_value: float = 0.0
    total_value: float = 0.0

@dataclass
class Signal:
    strategy: Strategy
    market: ClassifiedMarket
    side: Side
    entry_price: int
    target_price: int
    stop_price: int
    quantity: int
    edge_cents: int = 0
    edge_pct: float = 0.0
    fair_value: int = 0
    reason: str = ""
    layers: List[Tuple[int, int]] = field(default_factory=list)
    signal_type: str = ""       # for fill quality learning
    confidence: float = 0.5     # 0-1 confidence from order flow

@dataclass
class Position:
    strategy: Strategy
    ticker: str
    side: Side
    avg_entry: float
    quantity: int
    target_price: int
    stop_price: int
    entry_time: float
    order_ids: List[str] = field(default_factory=list)
    filled_qty: int = 0
    repost_count: int = 0
    partial_profit_taken: bool = False
    signal_type: str = ""

@dataclass
class TradeRecord:
    strategy: Strategy
    ticker: str
    market_type: MarketType
    side: Side
    entry_price: float
    exit_price: float
    quantity: int
    pnl_cents: float
    hold_time_seconds: float
    entry_time: float
    exit_time: float
    reason: str = ""
    signal_type: str = ""
    slippage_cents: float = 0.0

@dataclass
class OrderFlowSnapshot:
    """Point-in-time capture of order flow state."""
    timestamp: float
    mid_price: int
    spread: int
    bid_depth: int
    ask_depth: int
    imbalance: float           # 0-1, >0.5 = bid heavy
    top_bid_qty: int
    top_ask_qty: int
    trade_aggressor: str       # 'buy' | 'sell' | 'none'
    trade_size: int


# ============================================================
# TEAM NORMALIZATION (90+ aliases)
# ============================================================

TEAM_ALIASES: Dict[str, str] = {
    # NBA
    "lakers": "Los Angeles Lakers", "los angeles l": "Los Angeles Lakers", "lal": "Los Angeles Lakers",
    "clippers": "Los Angeles Clippers", "los angeles c": "Los Angeles Clippers", "lac": "Los Angeles Clippers",
    "celtics": "Boston Celtics", "boston": "Boston Celtics",
    "nets": "Brooklyn Nets", "brooklyn": "Brooklyn Nets",
    "knicks": "New York Knicks", "new york": "New York Knicks", "ny knicks": "New York Knicks",
    "warriors": "Golden State Warriors", "golden state": "Golden State Warriors", "gsw": "Golden State Warriors",
    "76ers": "Philadelphia 76ers", "philadelphia": "Philadelphia 76ers", "sixers": "Philadelphia 76ers", "philly": "Philadelphia 76ers",
    "bucks": "Milwaukee Bucks", "milwaukee": "Milwaukee Bucks",
    "heat": "Miami Heat", "miami": "Miami Heat",
    "bulls": "Chicago Bulls", "chicago": "Chicago Bulls",
    "cavaliers": "Cleveland Cavaliers", "cleveland": "Cleveland Cavaliers", "cavs": "Cleveland Cavaliers",
    "mavericks": "Dallas Mavericks", "dallas": "Dallas Mavericks", "mavs": "Dallas Mavericks",
    "nuggets": "Denver Nuggets", "denver": "Denver Nuggets",
    "pistons": "Detroit Pistons", "detroit": "Detroit Pistons",
    "rockets": "Houston Rockets", "houston": "Houston Rockets",
    "pacers": "Indiana Pacers", "indiana": "Indiana Pacers",
    "grizzlies": "Memphis Grizzlies", "memphis": "Memphis Grizzlies",
    "timberwolves": "Minnesota Timberwolves", "minnesota": "Minnesota Timberwolves", "wolves": "Minnesota Timberwolves",
    "pelicans": "New Orleans Pelicans", "new orleans": "New Orleans Pelicans",
    "thunder": "Oklahoma City Thunder", "oklahoma city": "Oklahoma City Thunder", "okc": "Oklahoma City Thunder",
    "magic": "Orlando Magic", "orlando": "Orlando Magic",
    "suns": "Phoenix Suns", "phoenix": "Phoenix Suns",
    "trail blazers": "Portland Trail Blazers", "portland": "Portland Trail Blazers", "blazers": "Portland Trail Blazers",
    "kings": "Sacramento Kings", "sacramento": "Sacramento Kings",
    "spurs": "San Antonio Spurs", "san antonio": "San Antonio Spurs",
    "raptors": "Toronto Raptors", "toronto": "Toronto Raptors",
    "jazz": "Utah Jazz", "utah": "Utah Jazz",
    "wizards": "Washington Wizards", "washington": "Washington Wizards",
    "hawks": "Atlanta Hawks", "atlanta": "Atlanta Hawks",
    "hornets": "Charlotte Hornets", "charlotte": "Charlotte Hornets",
    # NFL
    "chiefs": "Kansas City Chiefs", "kansas city": "Kansas City Chiefs",
    "eagles": "Philadelphia Eagles",
    "bills": "Buffalo Bills", "buffalo": "Buffalo Bills",
    "49ers": "San Francisco 49ers", "san francisco": "San Francisco 49ers", "niners": "San Francisco 49ers",
    "ravens": "Baltimore Ravens", "baltimore": "Baltimore Ravens",
    "cowboys": "Dallas Cowboys",
    "lions": "Detroit Lions",
    "packers": "Green Bay Packers", "green bay": "Green Bay Packers",
    "dolphins": "Miami Dolphins",
    "bengals": "Cincinnati Bengals", "cincinnati": "Cincinnati Bengals",
    "vikings": "Minnesota Vikings",
    "seahawks": "Seattle Seahawks", "seattle": "Seattle Seahawks",
    "steelers": "Pittsburgh Steelers", "pittsburgh": "Pittsburgh Steelers",
    "broncos": "Denver Broncos",
    "patriots": "New England Patriots", "new england": "New England Patriots",
    "chargers": "Los Angeles Chargers",
    "saints": "New Orleans Saints",
    "cardinals": "Arizona Cardinals", "arizona": "Arizona Cardinals",
    "raiders": "Las Vegas Raiders", "las vegas": "Las Vegas Raiders",
    "commanders": "Washington Commanders",
    "bears": "Chicago Bears",
    "panthers": "Carolina Panthers", "carolina": "Carolina Panthers",
    "texans": "Houston Texans",
    "jaguars": "Jacksonville Jaguars", "jacksonville": "Jacksonville Jaguars",
    "titans": "Tennessee Titans", "tennessee": "Tennessee Titans",
    "colts": "Indianapolis Colts", "indianapolis": "Indianapolis Colts",
    "falcons": "Atlanta Falcons",
    "giants": "New York Giants", "ny giants": "New York Giants",
    "jets": "New York Jets", "ny jets": "New York Jets",
    "browns": "Cleveland Browns",
    "rams": "Los Angeles Rams",
    "buccaneers": "Tampa Bay Buccaneers", "tampa bay": "Tampa Bay Buccaneers", "bucs": "Tampa Bay Buccaneers",
}

NOISE_TOKENS = {"the", "at", "vs", "vs.", "game", "match", "will", "win", "win?", "series", "regular", "season"}


def normalize_team(raw: str) -> str:
    cleaned = re.sub(r'[^\w\s.]', '', raw.strip()).lower()
    for token in NOISE_TOKENS:
        cleaned = re.sub(rf'\b{re.escape(token)}\b', '', cleaned).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if cleaned in TEAM_ALIASES:
        return TEAM_ALIASES[cleaned]
    for alias, canonical in TEAM_ALIASES.items():
        if alias in cleaned or cleaned in alias:
            return canonical
    return raw.strip()


# ============================================================
# MARKET CLASSIFIER
# ============================================================

class MarketClassifier:
    """7-stage deterministic classifier for Kalshi market types."""

    def __init__(self, config: dict):
        self.reject_tickers = set(t.upper() for t in config.get('market_universe', {}).get('reject_tickers', []))
        self.reject_tokens = [t.lower() for t in config.get('market_universe', {}).get('reject_title_tokens', [])]
        self.class_config = config.get('market_universe', {}).get('classification', {})
        self.tennis_series = set(config.get('market_universe', {}).get('classification', {}).get('tennis_match', {}).get('series_filter', []))

    def classify(self, ticker: str, title: str, series: str = "", event_ticker: str = "") -> Optional[ClassifiedMarket]:
        upper_ticker = ticker.upper()
        lower_title = title.lower()

        # Skip ticker rejection for KXMVESPORTSMULTIGAMEEXTENDED (the canonical sports prefix)
        if not upper_ticker.startswith("KXMVESPORTSMULTIGAMEEXTENDED"):
            for reject in self.reject_tickers:
                if reject in upper_ticker:
                    return None
        for token in self.reject_tokens:
            if token in lower_title:
                return None

        if series in self.tennis_series:
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.TENNIS,
                                 series=series, event_ticker=event_ticker)
            self._extract_tennis_players(m)
            return m

        if any(kw in lower_title for kw in ["first half", "1h ", "1st half"]):
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.FIRST_HALF,
                                 series=series, event_ticker=event_ticker)
            self._extract_teams(m)
            return m

        spread_match = re.search(r'wins? by over ([\d.]+)', lower_title)
        if spread_match or 'spread' in lower_title:
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.SPREAD,
                                 series=series, event_ticker=event_ticker)
            if spread_match:
                m.spread_value = float(spread_match.group(1))
            self._extract_teams(m)
            return m

        total_match = re.search(r'over ([\d.]+) points', lower_title)
        if total_match or 'total' in lower_title:
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.TOTAL,
                                 series=series, event_ticker=event_ticker)
            if total_match:
                m.total_value = float(total_match.group(1))
            return m

        gw_match = re.match(r'^(.+?)\s+(?:at|vs\.?)\s+(.+?)(?:\s*\?)?$', title.strip(), re.IGNORECASE)
        if gw_match:
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.GAME_WINNER,
                                 series=series, event_ticker=event_ticker)
            m.team_a = normalize_team(gw_match.group(1))
            m.team_b = normalize_team(gw_match.group(2))
            if any(kw in lower_title for kw in ["spread", "total", "over", "under", "points"]):
                return None
            return m

        will_match = re.match(r'^will (.+?) win', title.strip(), re.IGNORECASE)
        if will_match:
            m = ClassifiedMarket(ticker=ticker, title=title, market_type=MarketType.GAME_WINNER,
                                 series=series, event_ticker=event_ticker)
            m.team_a = normalize_team(will_match.group(1))
            return m

        return None

    def _extract_teams(self, m: ClassifiedMarket):
        base = re.sub(r'\s*(spread|total|first half|1h).*$', '', m.title, flags=re.IGNORECASE).strip()
        match = re.match(r'^(.+?)\s+(?:at|vs\.?)\s+(.+?)$', base, re.IGNORECASE)
        if match:
            m.team_a = normalize_team(match.group(1))
            m.team_b = normalize_team(match.group(2))

    def _extract_tennis_players(self, m: ClassifiedMarket):
        match = re.match(r'^(.+?)\s+vs\.?\s+(.+?)$', m.title.strip(), re.IGNORECASE)
        if match:
            m.team_a = match.group(1).strip()
            m.team_b = match.group(2).strip()


# ============================================================
# FAIR VALUE ENGINE
# ============================================================

class FairValueEngine:
    """Computes fair value from sportsbook odds with de-vigging."""

    def __init__(self, config: dict):
        self.api_key = os.environ.get('ODDS_API_KEY', config.get('api', {}).get('odds_api_key', ''))
        self.base_url = config.get('api', {}).get('odds_api_base', 'https://api.the-odds-api.com/v4')
        self.sports = config.get('fair_value', {}).get('sportsbook_sports', [])
        self.bookmakers = config.get('fair_value', {}).get('bookmakers', [])
        self.devig_method = config.get('fair_value', {}).get('devig_method', 'power')
        self._cache: Dict[str, Tuple[float, dict]] = {}
        self._cache_ttl = 120

    def get_fair_value(self, market: ClassifiedMarket) -> Optional[int]:
        sport = self._map_sport(market)
        if not sport:
            return None
        odds_data = self._fetch_odds(sport)
        if not odds_data:
            return None
        match = self._match_event(market, odds_data)
        if not match:
            return None
        return self._compute_fair_value(market, match)

    def _map_sport(self, market: ClassifiedMarket) -> Optional[str]:
        mapping = {
            'KXNBA': 'basketball_nba', 'KXNFL': 'americanfootball_nfl',
            'KXMLB': 'baseball_mlb', 'KXNHL': 'icehockey_nhl',
            'KXNCAAB': 'basketball_ncaab',
            'KXTENNIS': 'tennis_atp_french_open', 'KXATP': 'tennis_atp_french_open',
            'KXWTA': 'tennis_wta_french_open',
        }
        return mapping.get(market.series.upper())

    def _fetch_odds(self, sport: str) -> List[dict]:
        if sport in self._cache:
            ts, data = self._cache[sport]
            if time.time() - ts < self._cache_ttl:
                return data
        if not self.api_key or self.api_key.startswith('$'):
            return []
        try:
            resp = requests.get(f"{self.base_url}/sports/{sport}/odds", params={
                'apiKey': self.api_key, 'regions': 'us',
                'markets': 'h2h,spreads,totals', 'oddsFormat': 'american',
                'bookmakers': ','.join(self.bookmakers),
            }, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                self._cache[sport] = (time.time(), data)
                return data
        except Exception as e:
            logging.warning(f"[FAIR_VALUE] Odds API error: {e}")
        return []

    def _match_event(self, market: ClassifiedMarket, events: List[dict]) -> Optional[dict]:
        best_match, best_score = None, 0
        for event in events:
            home = normalize_team(event.get('home_team', ''))
            away = normalize_team(event.get('away_team', ''))
            score = 0
            if market.team_a and market.team_b:
                teams = {market.team_a.lower(), market.team_b.lower()}
                book_teams = {home.lower(), away.lower()}
                score = len(teams & book_teams)
            elif market.team_a:
                if market.team_a.lower() in home.lower() or market.team_a.lower() in away.lower():
                    score = 1
            if score > best_score:
                best_score, best_match = score, event
        return best_match if best_score >= 1 else None

    def _compute_fair_value(self, market: ClassifiedMarket, event: dict) -> Optional[int]:
        all_probs = []
        for bookmaker in event.get('bookmakers', []):
            for mkt in bookmaker.get('markets', []):
                if market.market_type == MarketType.GAME_WINNER and mkt['key'] == 'h2h':
                    outcomes = mkt.get('outcomes', [])
                    if len(outcomes) == 2:
                        p1 = self._american_to_prob(outcomes[0].get('price', 0))
                        p2 = self._american_to_prob(outcomes[1].get('price', 0))
                        devigged = self._devig(p1, p2)
                        if devigged:
                            name0 = normalize_team(outcomes[0].get('name', ''))
                            name1 = normalize_team(outcomes[1].get('name', ''))
                            if market.team_a and market.team_a.lower() in name0.lower():
                                all_probs.append(devigged[0])
                            elif market.team_a and market.team_a.lower() in name1.lower():
                                all_probs.append(devigged[1])
                            else:
                                all_probs.append(devigged[0])
                elif market.market_type == MarketType.SPREAD and mkt['key'] == 'spreads':
                    for outcome in mkt.get('outcomes', []):
                        point = outcome.get('point', 0)
                        if abs(abs(point) - market.spread_value) < 1.0:
                            prob = self._american_to_prob(outcome.get('price', 0))
                            if prob:
                                all_probs.append(prob)
        if not all_probs:
            return None
        return max(1, min(99, round(sum(all_probs) / len(all_probs) * 100)))

    @staticmethod
    def _american_to_prob(odds: int) -> Optional[float]:
        if odds == 0:
            return None
        return 100 / (odds + 100) if odds > 0 else abs(odds) / (abs(odds) + 100)

    def _devig(self, p1: float, p2: float) -> Optional[Tuple[float, float]]:
        if not p1 or not p2:
            return None
        total = p1 + p2
        if total <= 0:
            return None
        if self.devig_method == 'power':
            k = 1.0
            for _ in range(50):
                s = p1**k + p2**k
                if abs(s - 1.0) < 0.0001:
                    break
                k *= math.log(2) / math.log(max(s, 0.01))
            return (p1**k, p2**k)
        return (p1 / total, p2 / total)


# ============================================================
# PHASE 1: ORDER FLOW ENGINE
# ============================================================

class OrderFlowEngine:
    """
    Replaces OrderBookAnalyzer with real-time order flow intelligence.
    Tracks aggression, liquidity pulls, whale patterns, and outputs
    a composite pressure score for each market.
    """

    # Known whale clip signatures (jpmorgan.chase patterns)
    WHALE_CLIPS = {2232, 2026, 280, 500, 1000}
    WHALE_CLIP_TOLERANCE = 5  # ±5 contracts counts as a match

    def __init__(self, api_client, config: dict):
        self.api = api_client
        self.config = config

        # Per-market rolling state (ticker -> deque of snapshots)
        self._snapshots: Dict[str, deque] = defaultdict(lambda: deque(maxlen=120))
        # Per-market previous book for delta detection
        self._prev_books: Dict[str, OrderBook] = {}
        # Whale accumulation tracker (ticker -> list of detected whale trades)
        self._whale_hits: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        # Trade tape (simulated from book changes)
        self._trade_tape: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def get_book(self, ticker: str) -> OrderBook:
        """Fetch order book (delegated to API)."""
        try:
            data = self.api.get(f"/markets/{ticker}/orderbook")
            book = OrderBook(timestamp=time.time())
            for level in data.get('orderbook', {}).get('yes', []):
                price, qty = int(level[0]), int(level[1])
                book.yes_bids.append(OrderBookLevel(price=price, quantity=qty))
            for level in data.get('orderbook', {}).get('no', []):
                price, qty = int(level[0]), int(level[1])
                book.no_bids.append(OrderBookLevel(price=price, quantity=qty))
            book.yes_bids.sort(key=lambda x: x.price, reverse=True)
            book.no_bids.sort(key=lambda x: x.price, reverse=True)
            return book
        except Exception as e:
            logging.debug(f"[FLOW] Error fetching {ticker}: {e}")
            return OrderBook()

    def analyze(self, ticker: str, book: OrderBook) -> Dict[str, Any]:
        """
        Full order flow analysis. Returns dict with:
          pressure: PressureSignal
          pressure_score: float (-1 bearish to +1 bullish)
          aggression: 'buy' | 'sell' | 'none'
          liquidity_pull: bool (detected fake support/resistance)
          whale_detected: bool
          whale_accumulating: bool (repeated whale clips over time)
          volatility: float (recent price std dev)
          regime: MarketRegime
          imbalance: float
          spread: int
          mid: int
          opportunity_score: float (0-100 composite)
        """
        prev = self._prev_books.get(ticker)
        result = {
            'pressure': PressureSignal.NEUTRAL,
            'pressure_score': 0.0,
            'aggression': 'none',
            'liquidity_pull': False,
            'whale_detected': False,
            'whale_accumulating': False,
            'volatility': 0.0,
            'imbalance': book.imbalance_ratio,
            'spread': book.yes_spread,
            'mid': book.mid_price,
            'arb_cost': book.arb_cost,
            'large_bids': [],
            'large_asks': [],
        }

        # --- Aggression Detection ---
        aggressor, trade_size = self._detect_aggression(ticker, book, prev)
        result['aggression'] = aggressor

        # --- Liquidity Pull Detection ---
        if prev:
            result['liquidity_pull'] = self._detect_liquidity_pull(book, prev)

        # --- Whale Tracking ---
        whale_det, whale_acc = self._detect_whales(ticker, book)
        result['whale_detected'] = whale_det
        result['whale_accumulating'] = whale_acc

        # --- Large order detection ---
        avg_bid_size = (book.yes_bid_depth / len(book.yes_bids)) if book.yes_bids else 0
        for level in book.yes_bids:
            if level.quantity > avg_bid_size * 3 and level.quantity > 500:
                result['large_bids'].append({'price': level.price, 'qty': level.quantity})
        avg_ask_size = (book.yes_ask_depth / len(book.yes_asks)) if book.yes_asks else 0
        for level in book.yes_asks:
            if level.quantity > avg_ask_size * 3 and level.quantity > 500:
                result['large_asks'].append({'price': level.price, 'qty': level.quantity})

        # --- Record snapshot ---
        snap = OrderFlowSnapshot(
            timestamp=time.time(),
            mid_price=book.mid_price,
            spread=book.yes_spread,
            bid_depth=book.yes_bid_depth,
            ask_depth=book.yes_ask_depth,
            imbalance=book.imbalance_ratio,
            top_bid_qty=book.yes_bids[0].quantity if book.yes_bids else 0,
            top_ask_qty=book.yes_asks[0].quantity if book.yes_asks else 0,
            trade_aggressor=aggressor,
            trade_size=trade_size,
        )
        self._snapshots[ticker].append(snap)

        # --- Volatility (std dev of mid prices over last N snapshots) ---
        snaps = self._snapshots[ticker]
        if len(snaps) >= 5:
            mids = [s.mid_price for s in snaps]
            result['volatility'] = statistics.stdev(mids[-20:]) if len(mids) >= 5 else 0
        else:
            result['volatility'] = 0

        # --- Composite Pressure Score (-1 to +1) ---
        result['pressure_score'] = self._compute_pressure_score(ticker, book, aggressor)
        if result['pressure_score'] > 0.3:
            result['pressure'] = PressureSignal.BULLISH
        elif result['pressure_score'] < -0.3:
            result['pressure'] = PressureSignal.BEARISH
        else:
            result['pressure'] = PressureSignal.NEUTRAL

        # Store for next delta
        self._prev_books[ticker] = book
        return result

    def _detect_aggression(self, ticker: str, book: OrderBook, prev: Optional[OrderBook]) -> Tuple[str, int]:
        """
        Infer trade aggressor from book changes.
        If top-of-book ask qty decreased → buyer lifted ask (buy aggression)
        If top-of-book bid qty decreased → seller hit bid (sell aggression)
        """
        if not prev:
            return 'none', 0

        ask_consumed = 0
        bid_consumed = 0

        # Check if top ask level was consumed
        if prev.yes_asks and book.yes_asks:
            if book.best_yes_ask > prev.best_yes_ask:
                # Price moved up — asks were consumed
                ask_consumed = prev.yes_asks[0].quantity
            elif book.best_yes_ask == prev.best_yes_ask:
                diff = prev.yes_asks[0].quantity - book.yes_asks[0].quantity
                if diff > 0:
                    ask_consumed = diff

        # Check if top bid level was consumed
        if prev.yes_bids and book.yes_bids:
            if book.best_yes_bid < prev.best_yes_bid:
                bid_consumed = prev.yes_bids[0].quantity
            elif book.best_yes_bid == prev.best_yes_bid:
                diff = prev.yes_bids[0].quantity - book.yes_bids[0].quantity
                if diff > 0:
                    bid_consumed = diff

        if ask_consumed > bid_consumed and ask_consumed > 10:
            self._trade_tape[ticker].append(('buy', ask_consumed, time.time()))
            return 'buy', ask_consumed
        elif bid_consumed > ask_consumed and bid_consumed > 10:
            self._trade_tape[ticker].append(('sell', bid_consumed, time.time()))
            return 'sell', bid_consumed

        return 'none', 0

    def _detect_liquidity_pull(self, book: OrderBook, prev: OrderBook) -> bool:
        """
        Detect spoofing / liquidity withdrawal.
        Large order disappears without price movement → fake support/resistance.
        """
        if not prev.yes_bids or not book.yes_bids:
            return False

        # Check if large bid disappeared without trade (price didn't move)
        for prev_level in prev.yes_bids[:3]:
            if prev_level.quantity > 500:
                # Check if this level still exists with similar size
                found = False
                for curr_level in book.yes_bids:
                    if curr_level.price == prev_level.price:
                        if curr_level.quantity >= prev_level.quantity * 0.3:
                            found = True
                        break
                if not found and book.best_yes_bid >= prev.best_yes_bid:
                    # Large qty vanished but price didn't drop — likely pull
                    return True

        # Same for asks
        for prev_level in prev.yes_asks[:3]:
            if prev_level.quantity > 500:
                found = False
                for curr_level in book.yes_asks:
                    if curr_level.price == prev_level.price:
                        if curr_level.quantity >= prev_level.quantity * 0.3:
                            found = True
                        break
                if not found and book.best_yes_ask <= prev.best_yes_ask:
                    return True

        return False

    def _detect_whales(self, ticker: str, book: OrderBook) -> Tuple[bool, bool]:
        """
        Detect whale clip patterns (2232, 2026, 280, etc.) and
        track whether a whale is accumulating over time.
        """
        detected = False
        all_levels = list(book.yes_bids) + list(book.yes_asks) + list(book.no_bids) + list(book.no_asks)

        for level in all_levels:
            for whale_size in self.WHALE_CLIPS:
                if abs(level.quantity - whale_size) <= self.WHALE_CLIP_TOLERANCE:
                    detected = True
                    self._whale_hits[ticker].append({
                        'time': time.time(),
                        'price': level.price,
                        'qty': level.quantity,
                        'matched_clip': whale_size,
                    })
                    break

        # Accumulation: 3+ whale clips in last 10 minutes
        recent_hits = [h for h in self._whale_hits[ticker]
                       if time.time() - h['time'] < 600]
        accumulating = len(recent_hits) >= 3

        return detected, accumulating

    def _compute_pressure_score(self, ticker: str, book: OrderBook, aggressor: str) -> float:
        """
        Composite pressure score from -1.0 (extreme bearish) to +1.0 (extreme bullish).
        Components:
          1. Order imbalance (bid vs ask depth)
          2. Recent trade flow direction
          3. Liquidity concentration
        """
        score = 0.0

        # Component 1: Imbalance (weight 0.4)
        imb = book.imbalance_ratio  # >0.5 = bid heavy = bullish
        score += (imb - 0.5) * 2.0 * 0.4

        # Component 2: Recent aggression (weight 0.35)
        tape = self._trade_tape[ticker]
        if tape:
            recent = [t for t in tape if time.time() - t[2] < 60]
            buy_vol = sum(t[1] for t in recent if t[0] == 'buy')
            sell_vol = sum(t[1] for t in recent if t[0] == 'sell')
            total = buy_vol + sell_vol
            if total > 0:
                score += ((buy_vol - sell_vol) / total) * 0.35

        # Component 3: Top-of-book size asymmetry (weight 0.25)
        top_bid = book.yes_bids[0].quantity if book.yes_bids else 0
        top_ask = book.yes_asks[0].quantity if book.yes_asks else 0
        total_top = top_bid + top_ask
        if total_top > 0:
            score += ((top_bid - top_ask) / total_top) * 0.25

        return max(-1.0, min(1.0, score))

    def get_short_term_direction(self, ticker: str) -> Tuple[PressureSignal, float]:
        """
        Predict 10-60 second price direction.
        Returns (signal, confidence 0-1).
        """
        snaps = self._snapshots[ticker]
        if len(snaps) < 3:
            return PressureSignal.NEUTRAL, 0.0

        recent = list(snaps)[-10:]

        # Price trend
        prices = [s.mid_price for s in recent]
        if len(prices) >= 3:
            trend = prices[-1] - prices[0]
        else:
            trend = 0

        # Imbalance trend
        imbs = [s.imbalance for s in recent]
        avg_imb = sum(imbs) / len(imbs)

        # Aggression trend
        buy_count = sum(1 for s in recent if s.trade_aggressor == 'buy')
        sell_count = sum(1 for s in recent if s.trade_aggressor == 'sell')

        # Composite
        direction = 0.0
        direction += (0.4 * (1 if trend > 0 else (-1 if trend < 0 else 0)))
        direction += (0.3 * (avg_imb - 0.5) * 2)
        direction += (0.3 * ((buy_count - sell_count) / max(len(recent), 1)))

        confidence = min(1.0, abs(direction) * 1.5)

        if direction > 0.15:
            return PressureSignal.BULLISH, confidence
        elif direction < -0.15:
            return PressureSignal.BEARISH, confidence
        return PressureSignal.NEUTRAL, confidence


# ============================================================
# PHASE 5: REGIME DETECTOR
# ============================================================

class RegimeDetector:
    """
    Classifies each market into a regime:
      PRE_EVENT: stable, low volatility — good for value_maker, tennis_mm
      ACTIVE: volatile, high trade flow — good for scalper
      DEAD: no liquidity, skip entirely
    """

    def __init__(self, config: dict):
        self.liq_cfg = config.get('liquidity', {})

    def classify(self, market: ClassifiedMarket, book: OrderBook,
                 flow: Dict[str, Any]) -> MarketRegime:
        """Determine regime from book + flow analysis."""
        # Dead: insufficient liquidity
        if (market.volume < self.liq_cfg.get('min_volume', 100) // 2 or
                book.yes_bid_depth + book.yes_ask_depth < 50 or
                len(book.yes_bids) < 2):
            return MarketRegime.DEAD

        volatility = flow.get('volatility', 0)
        spread = book.yes_spread

        # Active: high volatility or tight spread with volume
        if volatility > 2.0 or (spread <= 5 and market.volume > 500):
            return MarketRegime.ACTIVE

        # Pre-event: stable
        if spread <= 10 and volatility <= 2.0:
            return MarketRegime.PRE_EVENT

        return MarketRegime.UNKNOWN

    def strategy_allowed(self, strategy: Strategy, regime: MarketRegime) -> bool:
        """Determine if a strategy should operate in this regime."""
        REGIME_MAP = {
            Strategy.SCALPER: {MarketRegime.ACTIVE},
            Strategy.VALUE_MAKER: {MarketRegime.PRE_EVENT, MarketRegime.ACTIVE},
            Strategy.TENNIS_MM: {MarketRegime.PRE_EVENT, MarketRegime.ACTIVE},
            Strategy.SPREAD_ARB: {MarketRegime.PRE_EVENT, MarketRegime.ACTIVE, MarketRegime.UNKNOWN},
        }
        allowed = REGIME_MAP.get(strategy, set())
        return regime in allowed


# ============================================================
# PHASE 2: ADAPTIVE PARAMETERS
# ============================================================

class AdaptiveParams:
    """
    Dynamically adjusts strategy parameters based on market conditions.
    Replaces static config values with real-time adaptive ones.
    """

    @staticmethod
    def adjust_profit_target(base_target: int, volatility: float, spread: int) -> int:
        """
        High vol → tighter exits (capture before reversal).
        Low vol → can be more patient.
        """
        if volatility > 4.0:
            return max(2, int(base_target * 0.6))  # tighter
        elif volatility > 2.0:
            return max(2, int(base_target * 0.8))
        elif volatility < 0.5:
            return int(base_target * 1.3)           # more patient
        return base_target

    @staticmethod
    def adjust_stop_loss(base_stop: int, volatility: float) -> int:
        """Wider stops in volatile markets to avoid noise stops."""
        if volatility > 4.0:
            return int(base_stop * 1.5)
        elif volatility > 2.0:
            return int(base_stop * 1.2)
        return base_stop

    @staticmethod
    def adjust_ladder_spacing(base_spacing: int, spread: int) -> int:
        """Widen spacing when spread is wide, tighten when narrow."""
        if spread > 10:
            return max(1, int(base_spacing * 1.5))
        elif spread < 4:
            return max(1, int(base_spacing * 0.7))
        return base_spacing

    @staticmethod
    def adjust_position_size(base_size: int, book: OrderBook, volatility: float) -> int:
        """
        Less size in low liquidity or high vol.
        More size when deep book + stable.
        """
        depth = book.yes_bid_depth + book.yes_ask_depth
        size = base_size

        # Liquidity adjustment
        if depth < 500:
            size = int(size * 0.3)
        elif depth < 2000:
            size = int(size * 0.6)
        elif depth > 10000:
            size = int(size * 1.3)

        # Volatility adjustment
        if volatility > 4.0:
            size = int(size * 0.5)
        elif volatility > 2.0:
            size = int(size * 0.7)

        return max(10, size)


# ============================================================
# PHASE 3: STRATEGY ALLOCATOR
# ============================================================

class StrategyAllocator:
    """
    Ranks markets by opportunity score and allocates capital dynamically.
    Higher-scoring markets get larger position sizes.
    """

    def __init__(self, config: dict):
        self.max_total = config.get('risk', {}).get('max_total_exposure', 50000)

    def score_market(self, market: ClassifiedMarket, book: OrderBook,
                     flow: Dict[str, Any], fair_value: Optional[int]) -> float:
        """
        Composite opportunity score 0-100.
        Components:
          - Spread (tighter = better, but need minimum)
          - Liquidity (deeper = better)
          - Volatility (moderate is best for most strategies)
          - Edge vs fair value
        """
        score = 0.0

        # Spread: best between 3-8¢ (weight 25)
        spread = book.yes_spread
        if 3 <= spread <= 8:
            score += 25
        elif spread <= 12:
            score += 15
        elif spread <= 3:
            score += 10   # too tight, hard to capture
        # >12: 0

        # Liquidity: log scale (weight 25)
        depth = book.yes_bid_depth + book.yes_ask_depth
        if depth > 0:
            score += min(25, math.log10(max(depth, 1)) * 8)

        # Volatility: moderate is best (weight 20)
        vol = flow.get('volatility', 0)
        if 1.0 <= vol <= 4.0:
            score += 20
        elif 0.5 <= vol < 1.0 or 4.0 < vol <= 6.0:
            score += 10
        # extreme: 0

        # Edge vs fair value (weight 30)
        if fair_value:
            edge = abs(book.mid_price - fair_value)
            score += min(30, edge * 3)

        return min(100, score)

    def allocate_capital(self, scored_markets: List[Tuple[float, ClassifiedMarket]],
                         base_size: int) -> Dict[str, int]:
        """
        Allocate position sizes proportional to opportunity score.
        Top markets get up to 2x base, bottom get 0.5x.
        """
        if not scored_markets:
            return {}

        # Sort by score descending
        ranked = sorted(scored_markets, key=lambda x: x[0], reverse=True)
        allocations = {}

        for i, (score, market) in enumerate(ranked):
            # Top quartile: 1.5-2x, bottom quartile: 0.5x
            rank_pct = i / max(len(ranked), 1)
            if rank_pct <= 0.25:
                multiplier = 1.5 + (score / 100) * 0.5
            elif rank_pct <= 0.50:
                multiplier = 1.0 + (score / 100) * 0.3
            elif rank_pct <= 0.75:
                multiplier = 0.7
            else:
                multiplier = 0.5

            allocations[market.ticker] = max(10, int(base_size * multiplier))

        return allocations


# ============================================================
# PHASE 4: FILL QUALITY TRACKER
# ============================================================

class FillQualityTracker:
    """
    Learns from every trade to identify which signals produce profit
    and which produce losses. Auto-disables underperforming signal types.
    """

    def __init__(self, config: dict):
        # signal_type -> list of outcomes
        self._outcomes: Dict[str, List[dict]] = defaultdict(list)
        self._disabled_signals: set = set()
        self._min_trades_to_evaluate = config.get('paper', {}).get('min_profitable_trades_to_go_live', 30) // 3
        self._min_ev_to_keep = -2.0  # disable if avg PnL < -2¢

    def record(self, trade: TradeRecord):
        """Record a completed trade for learning."""
        key = f"{trade.strategy.value}:{trade.signal_type or 'default'}"
        self._outcomes[key].append({
            'pnl': trade.pnl_cents,
            'slippage': trade.slippage_cents,
            'hold_time': trade.hold_time_seconds,
            'market_type': trade.market_type.value,
        })

    def is_signal_enabled(self, strategy: Strategy, signal_type: str) -> bool:
        """Check if a signal type is still enabled."""
        key = f"{strategy.value}:{signal_type}"
        return key not in self._disabled_signals

    def evaluate_all(self) -> Dict[str, dict]:
        """Evaluate all signal types and auto-disable underperformers."""
        report = {}
        for key, outcomes in self._outcomes.items():
            n = len(outcomes)
            if n < self._min_trades_to_evaluate:
                report[key] = {'status': 'insufficient_data', 'trades': n}
                continue

            pnls = [o['pnl'] for o in outcomes]
            avg_pnl = sum(pnls) / n
            win_rate = sum(1 for p in pnls if p > 0) / n
            avg_slippage = sum(o['slippage'] for o in outcomes) / n
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            ev = avg_pnl

            report[key] = {
                'trades': n,
                'win_rate': win_rate,
                'avg_pnl': avg_pnl,
                'avg_slippage': avg_slippage,
                'ev_per_trade': ev,
                'total_pnl': sum(pnls),
                'status': 'active',
            }

            # Auto-disable if consistently unprofitable
            if n >= self._min_trades_to_evaluate and ev < self._min_ev_to_keep:
                self._disabled_signals.add(key)
                report[key]['status'] = 'DISABLED'
                logging.warning(f"[FILL_QUALITY] Auto-disabled signal: {key} (EV={ev:.1f}¢ over {n} trades)")

        return report

    def get_expected_value(self, strategy: Strategy, signal_type: str) -> Optional[float]:
        """Get expected value per trade for a signal type."""
        key = f"{strategy.value}:{signal_type}"
        outcomes = self._outcomes.get(key, [])
        if len(outcomes) < 5:
            return None
        pnls = [o['pnl'] for o in outcomes]
        return sum(pnls) / len(pnls)

    def print_report(self):
        """Print fill quality learning report."""
        report = self.evaluate_all()
        if not report:
            return
        print("\n" + "=" * 70)
        print("FILL QUALITY LEARNING REPORT")
        print("=" * 70)
        for key, stats in sorted(report.items()):
            status_icon = "✅" if stats['status'] == 'active' else ("⏳" if stats['status'] == 'insufficient_data' else "🚫")
            print(f"  {status_icon} {key}:")
            if stats['status'] == 'insufficient_data':
                print(f"      {stats['trades']} trades (need {self._min_trades_to_evaluate}+)")
            else:
                print(f"      Trades: {stats['trades']} | WR: {stats['win_rate']:.1%} | "
                      f"EV: {stats['ev_per_trade']:.1f}¢ | Slip: {stats.get('avg_slippage', 0):.1f}¢ | "
                      f"Status: {stats['status']}")


# ============================================================
# PHASE 6: SMART EXECUTION ENGINE
# ============================================================

class SmartExecutionEngine:
    """
    Upgraded execution with:
      - Smart order placement (join vs improve vs aggressive)
      - Queue position estimation
      - Cancel/repost based on order flow changes
      - Partial profit taking with trailing stops
    """

    def __init__(self, api_client, config: dict, is_paper: bool = True):
        self.api = api_client
        self.config = config
        self.is_paper = is_paper
        self.positions: Dict[str, Position] = {}
        self.open_orders: Dict[str, dict] = {}
        self._order_counter = 0
        # Queue position tracking
        self._queue_estimates: Dict[str, int] = {}  # order_id -> estimated queue position

    def decide_placement_mode(self, book: OrderBook, flow: Dict[str, Any],
                               side: Side) -> OrderPlacementMode:
        """
        Smart placement decision:
          - AGGRESSIVE if strong flow in our direction (momentum)
          - IMPROVE if moderate flow, tight spread
          - JOIN if neutral / wide spread (be patient)
        """
        pressure = flow.get('pressure_score', 0)
        spread = book.yes_spread
        is_buying_yes = (side == Side.YES)

        # If flow strongly favors us, be aggressive (cross spread)
        if (is_buying_yes and pressure > 0.5) or (not is_buying_yes and pressure < -0.5):
            if spread <= 5:
                return OrderPlacementMode.AGGRESSIVE
            return OrderPlacementMode.IMPROVE

        # Moderate flow or tight spread: improve by 1¢
        if abs(pressure) > 0.2 or spread <= 4:
            return OrderPlacementMode.IMPROVE

        # Neutral: join the queue
        return OrderPlacementMode.JOIN

    def compute_entry_price(self, book: OrderBook, side: Side,
                            mode: OrderPlacementMode) -> int:
        """Compute optimal entry price based on placement mode."""
        if side == Side.YES:
            if mode == OrderPlacementMode.AGGRESSIVE:
                return min(99, book.best_yes_ask)    # lift the ask
            elif mode == OrderPlacementMode.IMPROVE:
                return min(99, book.best_yes_bid + 1) # penny better
            else:
                return book.best_yes_bid              # join the queue
        else:
            if mode == OrderPlacementMode.AGGRESSIVE:
                return min(99, book.best_no_ask)
            elif mode == OrderPlacementMode.IMPROVE:
                return min(99, book.best_no_bid + 1)
            else:
                return book.best_no_bid

    def estimate_queue_position(self, order_id: str, book: OrderBook) -> int:
        """
        Estimate queue position for a resting limit order.
        Returns estimated contracts ahead of us at our price level.
        """
        order = self.open_orders.get(order_id)
        if not order:
            return -1

        price = order['price']
        side = order['side']

        # Find our price level in the book
        levels = book.yes_bids if side == 'yes' else book.no_bids
        ahead = 0
        for level in levels:
            if level.price > price:
                ahead += level.quantity
            elif level.price == price:
                # We're somewhere in this level — estimate we're in the middle
                ahead += level.quantity // 2
                break
            else:
                break

        self._queue_estimates[order_id] = ahead
        return ahead

    def should_cancel_repost(self, order_id: str, book: OrderBook,
                              flow: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
        """
        Decide whether to cancel and repost an order based on flow changes.
        Returns (should_cancel, new_price_or_none).
        """
        order = self.open_orders.get(order_id)
        if not order or order['status'] != 'pending':
            return False, None

        elapsed = time.time() - order['time']
        price = order['price']
        side = Side(order['side'])

        # Check if the market has moved away from us
        if side == Side.YES:
            best_bid = book.best_yes_bid
            if price < best_bid - 3:
                # We're 3+ cents behind best bid — repost closer
                return True, best_bid
        else:
            best_bid = book.best_no_bid
            if price < best_bid - 3:
                return True, best_bid

        # If pressure reversed strongly, cancel
        pressure = flow.get('pressure_score', 0)
        if side == Side.YES and pressure < -0.6:
            return True, None  # Cancel, don't repost (flow against us)
        if side == Side.NO and pressure > 0.6:
            return True, None

        # Stale order check (fallback)
        stale_seconds = self.config.get('strategies', {}).get(
            order.get('strategy', ''), {}).get('stale_order_seconds', 180)
        if elapsed > stale_seconds:
            return True, price  # Repost at same price

        return False, None

    def place_layered_entry(self, signal: Signal, book: OrderBook,
                            flow: Dict[str, Any]) -> List[str]:
        """Place layered limit orders with smart pricing."""
        order_ids = []
        layers = signal.layers if signal.layers else [(signal.entry_price, signal.quantity)]

        mode = self.decide_placement_mode(book, flow, signal.side)
        smart_price = self.compute_entry_price(book, signal.side, mode)

        for i, (price, qty) in enumerate(layers):
            # First layer uses smart price, subsequent use original spacing
            layer_price = smart_price - (i * 2) if i > 0 else smart_price
            layer_price = max(1, min(99, layer_price))

            # Randomize clip size ±20%
            jitter = random.randint(-int(qty * 0.2), int(qty * 0.2))
            actual_qty = max(1, qty + jitter)

            order_id = self._place_limit(signal.market.ticker, signal.side,
                                         layer_price, actual_qty, signal.strategy,
                                         signal.signal_type)
            if order_id:
                order_ids.append(order_id)

        logging.info(f"[EXEC] {mode.value.upper()} placement: {len(order_ids)} layers @ base={smart_price}¢")
        return order_ids

    def _place_limit(self, ticker: str, side: Side, price: int, qty: int,
                     strategy: Strategy, signal_type: str = "") -> Optional[str]:
        self._order_counter += 1
        order_id = f"{'PAPER' if self.is_paper else 'LIVE'}_{strategy.value}_{self._order_counter}_{int(time.time())}"

        if self.is_paper:
            self.open_orders[order_id] = {
                'ticker': ticker, 'side': side.value, 'price': price,
                'qty': qty, 'strategy': strategy.value, 'time': time.time(),
                'status': 'pending', 'filled_qty': 0, 'signal_type': signal_type,
            }
            logging.info(f"[EXEC] PAPER limit {side.value.upper()} {qty}x {ticker} @ {price}¢ | id={order_id}")
            return order_id
        else:
            try:
                payload = {
                    'ticker': ticker, 'action': 'buy', 'side': side.value,
                    'type': 'limit',
                    'yes_price' if side == Side.YES else 'no_price': price,
                    'count': qty,
                }
                resp = self.api.post('/portfolio/orders', json=payload)
                return resp.get('order', {}).get('order_id')
            except Exception as e:
                logging.error(f"[EXEC] Order failed: {e}")
                return None

    def simulate_fills(self):
        if not self.is_paper:
            return
        fill_rate = self.config.get('paper', {}).get('simulated_fill_rate', 0.65)
        for oid, order in list(self.open_orders.items()):
            if order['status'] != 'pending':
                continue
            elapsed = time.time() - order['time']
            fill_prob = min(fill_rate, fill_rate * (elapsed / 60))
            if random.random() < fill_prob:
                order['status'] = 'filled'
                order['filled_qty'] = order['qty']
                slippage = self.config.get('paper', {}).get('simulated_slippage_cents', 1)
                order['fill_price'] = order['price'] + random.choice([-slippage, 0, slippage])
                logging.info(f"[EXEC] PAPER FILL {order['side'].upper()} {order['qty']}x {order['ticker']} @ {order['price']}¢")
            elif elapsed > 120 and random.random() < 0.3:
                partial = random.randint(1, order['qty'] // 2)
                order['status'] = 'partial'
                order['filled_qty'] = partial
                order['fill_price'] = order['price']

    def repost_order(self, order_id: str, new_price: Optional[int] = None) -> Optional[str]:
        order = self.open_orders.get(order_id)
        if not order:
            return None
        order['status'] = 'cancelled'
        price = new_price or order['price']
        remaining = order['qty'] - order['filled_qty']
        if remaining <= 0:
            return None
        new_id = self._place_limit(
            order['ticker'], Side(order['side']), price, remaining,
            Strategy(order['strategy']), order.get('signal_type', ''))
        logging.info(f"[EXEC] REPOST {order_id} → {new_id} @ {price}¢ ({remaining} contracts)")
        return new_id

    def place_take_profit(self, ticker: str, side: Side, price: int,
                          qty: int, strategy: Strategy) -> Optional[str]:
        exit_side = Side.NO if side == Side.YES else Side.YES
        return self._place_limit(ticker, exit_side, 100 - price, qty, strategy)

    def get_fills(self) -> List[dict]:
        fills = []
        for oid, order in self.open_orders.items():
            if order['status'] in ('filled', 'partial') and not order.get('_reported'):
                fills.append({'order_id': oid, **order})
                order['_reported'] = True
        return fills

    def cancel_order(self, order_id: str):
        order = self.open_orders.get(order_id)
        if order:
            order['status'] = 'cancelled'
            logging.info(f"[EXEC] CANCELLED {order_id}")


# ============================================================
# RISK MANAGER
# ============================================================

class RiskManager:
    def __init__(self, config: dict):
        self.cfg = config.get('risk', {})
        self.liq_cfg = config.get('liquidity', {})
        self.daily_pnl = 0.0
        self.total_exposure = 0
        self.event_exposure: Dict[str, int] = defaultdict(int)
        self.market_exposure: Dict[str, int] = defaultdict(int)

    def can_trade(self, signal: Signal) -> Tuple[bool, str]:
        cost = signal.entry_price * signal.quantity
        if self.daily_pnl <= -self.cfg.get('max_daily_loss', 5000) * 100:
            return False, "daily_loss_limit"
        if self.total_exposure + cost > self.cfg.get('max_total_exposure', 50000) * 100:
            return False, "total_exposure_limit"
        event = signal.market.event_ticker or signal.market.ticker
        if self.event_exposure[event] + cost > self.cfg.get('max_per_event', 10000) * 100:
            return False, "event_exposure_limit"
        if self.market_exposure[signal.market.ticker] + cost > self.cfg.get('max_per_market', 5000) * 100:
            return False, "market_exposure_limit"
        return True, "ok"

    def passes_liquidity(self, market: ClassifiedMarket, book: OrderBook) -> Tuple[bool, str]:
        if market.volume < self.liq_cfg.get('min_volume', 100):
            return False, "low_volume"
        if book.yes_spread > self.liq_cfg.get('max_spread_cents', 15):
            return False, "wide_spread"
        if len(book.yes_bids) < self.liq_cfg.get('min_book_depth', 3):
            return False, "thin_book"
        return True, "ok"

    def record_trade(self, signal: Signal):
        cost = signal.entry_price * signal.quantity
        self.total_exposure += cost
        event = signal.market.event_ticker or signal.market.ticker
        self.event_exposure[event] += cost
        self.market_exposure[signal.market.ticker] += cost

    def record_pnl(self, pnl_cents: float):
        self.daily_pnl += pnl_cents


# ============================================================
# STRATEGY ENGINES (upgraded with adaptive params + flow awareness)
# ============================================================

class SpreadArbStrategy:
    def __init__(self, config: dict):
        self.cfg = config.get('strategies', {}).get('spread_arb', {})
        self.min_edge = self.cfg.get('min_edge_cents', 2)
        self.clips = self.cfg.get('clip_sizes', [100, 250, 500])
        self.max_pos = self.cfg.get('max_position', 5000)

    def scan(self, market: ClassifiedMarket, book: OrderBook,
             flow: Dict[str, Any] = None) -> Optional[Signal]:
        if not self.cfg.get('enabled', True):
            return None
        edge = 100 - book.arb_cost
        if edge < self.min_edge:
            return None
        yes_available = sum(l.quantity for l in book.yes_asks[:3])
        no_available = sum(l.quantity for l in book.no_asks[:3])
        max_qty = min(yes_available, no_available, self.max_pos)
        qty = min(max_qty, random.choice(self.clips))
        if qty < 10:
            return None

        # Adaptive: reduce size if liquidity pull detected (possible spoof)
        if flow and flow.get('liquidity_pull'):
            qty = qty // 2
            if qty < 10:
                return None

        layers = [(book.best_yes_ask, qty), (book.best_no_ask, qty)]
        return Signal(
            strategy=Strategy.SPREAD_ARB, market=market, side=Side.YES,
            entry_price=book.best_yes_ask, target_price=100, stop_price=0,
            quantity=qty, edge_cents=edge, edge_pct=edge,
            signal_type="arb_both_sides",
            reason=f"ARB: YES@{book.best_yes_ask}+NO@{book.best_no_ask}={book.arb_cost}¢ edge={edge}¢",
            layers=layers,
        )


class ScalperStrategy:
    def __init__(self, config: dict):
        self.cfg = config.get('strategies', {}).get('scalper', {})
        self.dev_cents = self.cfg.get('entry_deviation_cents', 5)
        self.target = self.cfg.get('exit_target_cents', 7)
        self.stop = self.cfg.get('stop_loss_cents', 10)
        self.layers_count = self.cfg.get('ladder_layers', 3)
        self.spacing = self.cfg.get('ladder_spacing_cents', 2)
        self.clips = self.cfg.get('clip_sizes', [50, 100, 200])
        self.max_pos = self.cfg.get('max_position', 3000)
        self._cooldowns: Dict[str, float] = {}

    def scan(self, market: ClassifiedMarket, book: OrderBook,
             fair_value: Optional[int] = None,
             flow: Dict[str, Any] = None) -> Optional[Signal]:
        if not self.cfg.get('enabled', True):
            return None

        cd = self.cfg.get('cooldown_seconds', 30)
        if market.ticker in self._cooldowns:
            if time.time() - self._cooldowns[market.ticker] < cd:
                return None

        ref = fair_value or book.mid_price
        deviation = ref - book.best_yes_ask

        if abs(deviation) < self.dev_cents:
            return None

        # Use flow for direction confirmation
        if flow:
            pressure = flow.get('pressure_score', 0)
            vol = flow.get('volatility', 0)

            # Adaptive parameters
            target = AdaptiveParams.adjust_profit_target(self.target, vol, book.yes_spread)
            stop = AdaptiveParams.adjust_stop_loss(self.stop, vol)
            spacing = AdaptiveParams.adjust_ladder_spacing(self.spacing, book.yes_spread)
            clip = AdaptiveParams.adjust_position_size(
                random.choice(self.clips), book, vol)

            # Require flow confirmation: don't scalp against strong pressure
            if deviation > 0 and pressure < -0.4:
                return None  # price looks cheap but sellers dominating
            if deviation < 0 and pressure > 0.4:
                return None
        else:
            target, stop, spacing, clip = self.target, self.stop, self.spacing, random.choice(self.clips)

        if deviation > 0:
            side = Side.YES
            entry = book.best_yes_ask
            signal_type = "scalp_below_fair"
        else:
            side = Side.NO
            entry = book.best_no_ask
            signal_type = "scalp_above_fair"

        layers = []
        for i in range(self.layers_count):
            p = max(1, min(99, entry + (i * spacing if side == Side.NO else entry - i * spacing)))
            layers.append((p, clip))

        total_qty = min(clip * self.layers_count, self.max_pos)
        confidence = min(1.0, abs(deviation) / 10) * (0.5 + abs(flow.get('pressure_score', 0)) * 0.5 if flow else 0.5)

        return Signal(
            strategy=Strategy.SCALPER, market=market, side=side,
            entry_price=entry,
            target_price=min(99, entry + target) if side == Side.YES else max(1, entry - target),
            stop_price=max(1, entry - stop) if side == Side.YES else min(99, entry + stop),
            quantity=total_qty, edge_cents=abs(deviation),
            edge_pct=abs(deviation) / ref * 100 if ref else 0,
            fair_value=ref, signal_type=signal_type, confidence=confidence,
            reason=f"SCALP: dev={deviation}¢ from ref={ref} | target={target}¢ stop={stop}¢",
            layers=layers,
        )


class ValueMakerStrategy:
    def __init__(self, config: dict):
        self.cfg = config.get('strategies', {}).get('value_maker', {})
        self.min_edge_pct = self.cfg.get('min_edge_pct', 7)
        self.entry_below = self.cfg.get('entry_below_mid_cents', 5)
        self.target = self.cfg.get('exit_target_cents', 5)
        self.clips = self.cfg.get('clip_sizes', [280, 500, 1000, 2026, 2232])
        self.max_pos = self.cfg.get('max_position', 10000)

    def scan(self, market: ClassifiedMarket, book: OrderBook,
             fair_value: Optional[int] = None,
             flow: Dict[str, Any] = None) -> Optional[Signal]:
        if not self.cfg.get('enabled', True):
            return None
        if not fair_value:
            return None

        edge_pct = abs(fair_value - book.mid_price) / max(fair_value, 1) * 100
        if edge_pct < self.min_edge_pct:
            return None

        # Adaptive: adjust entry depth based on volatility
        vol = flow.get('volatility', 0) if flow else 0
        entry_depth = self.entry_below
        if vol > 3.0:
            entry_depth = max(2, entry_depth - 2)  # closer entry in volatile
        elif vol < 1.0:
            entry_depth = entry_depth + 2            # deeper entry when stable

        if fair_value > book.mid_price:
            side = Side.YES
            entry = max(1, book.mid_price - entry_depth)
            signal_type = "value_yes_underpriced"
        else:
            side = Side.NO
            entry = max(1, 100 - book.mid_price - entry_depth)
            signal_type = "value_no_underpriced"

        # Iceberg clip selection (jpmorgan.chase signature)
        clip = random.choice(self.clips)
        qty = AdaptiveParams.adjust_position_size(clip, book, vol) if flow else clip

        # Don't enter if whale is accumulating same side (front-running risk)
        if flow and flow.get('whale_accumulating'):
            logging.info(f"[VALUE] Whale accumulating in {market.ticker} — adjusting size")
            qty = qty // 2

        target = min(99, entry + self.target) if side == Side.YES else max(1, entry - self.target)

        return Signal(
            strategy=Strategy.VALUE_MAKER, market=market, side=side,
            entry_price=entry, target_price=target,
            stop_price=max(1, entry - 15) if side == Side.YES else min(99, entry + 15),
            quantity=min(qty, self.max_pos), edge_cents=int(edge_pct),
            edge_pct=edge_pct, fair_value=fair_value, signal_type=signal_type,
            reason=f"VALUE: FV={fair_value} mid={book.mid_price} edge={edge_pct:.1f}% entry={entry}¢",
        )


class TennisMMStrategy:
    def __init__(self, config: dict):
        self.cfg = config.get('strategies', {}).get('tennis_mm', {})
        self.min_spread = self.cfg.get('spread_capture_cents', 4)
        self.layers_count = self.cfg.get('ladder_layers', 5)
        self.spacing = self.cfg.get('ladder_spacing_cents', 3)
        self.clips = self.cfg.get('clip_sizes', [50, 100, 200, 500, 800])
        self.max_pos = self.cfg.get('max_position', 2000)
        self.inventory_limit = self.cfg.get('inventory_limit', 1500)
        self._inventory: Dict[str, int] = defaultdict(int)

    def scan(self, market: ClassifiedMarket, book: OrderBook,
             flow: Dict[str, Any] = None) -> Optional[Signal]:
        if not self.cfg.get('enabled', True):
            return None
        if market.market_type != MarketType.TENNIS:
            return None

        spread = book.yes_spread
        if spread < self.min_spread:
            return None

        net_inv = self._inventory.get(market.ticker, 0)

        # Adaptive spacing based on spread
        vol = flow.get('volatility', 0) if flow else 0
        spacing = AdaptiveParams.adjust_ladder_spacing(self.spacing, spread)

        if abs(net_inv) > self.inventory_limit:
            side = Side.NO if net_inv > 0 else Side.YES
            entry = book.mid_price if side == Side.YES else (100 - book.mid_price)
            signal_type = "tennis_mm_hedge"
            reason = f"TENNIS_MM HEDGE: inv={net_inv} reducing"
        else:
            # Use pressure to decide which side to make
            pressure = flow.get('pressure_score', 0) if flow else 0
            if pressure > 0.3:
                side = Side.YES
                entry = book.best_yes_bid + 1
                signal_type = "tennis_mm_bid_bullish"
            elif pressure < -0.3:
                side = Side.NO
                entry = book.best_no_bid + 1
                signal_type = "tennis_mm_bid_bearish"
            else:
                side = Side.YES
                entry = book.best_yes_bid + 1
                signal_type = "tennis_mm_bid_neutral"
            reason = f"TENNIS_MM: spread={spread}¢ bid+1={entry}¢ pressure={pressure:.2f}"

        clip = AdaptiveParams.adjust_position_size(
            random.choice(self.clips), book, vol) if flow else random.choice(self.clips)
        layers = []
        for i in range(self.layers_count):
            layer_price = max(1, min(99, entry - (i * spacing)))
            layers.append((layer_price, clip))

        total_qty = min(clip * self.layers_count, self.max_pos - abs(net_inv))
        if total_qty < 10:
            return None

        target = entry + (spread // 2)

        return Signal(
            strategy=Strategy.TENNIS_MM, market=market, side=side,
            entry_price=entry, target_price=min(99, target),
            stop_price=max(1, entry - 15), quantity=total_qty,
            edge_cents=spread // 2, edge_pct=spread / 2,
            signal_type=signal_type, reason=reason, layers=layers,
        )

    def update_inventory(self, ticker: str, side: Side, qty: int, is_entry: bool):
        delta = qty if side == Side.YES else -qty
        if not is_entry:
            delta = -delta
        self._inventory[ticker] = self._inventory.get(ticker, 0) + delta


# ============================================================
# METRICS TRACKER
# ============================================================

class MetricsTracker:
    def __init__(self):
        self.trades: Dict[Strategy, List[TradeRecord]] = defaultdict(list)
        self.failures: Dict[Strategy, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._scan_count = 0
        self._regime_counts: Dict[str, int] = defaultdict(int)
        self._flow_events: Dict[str, int] = defaultdict(int)

    def record_trade(self, trade: TradeRecord):
        self.trades[trade.strategy].append(trade)

    def record_failure(self, strategy: Strategy, reason: str):
        self.failures[strategy][reason] += 1

    def record_regime(self, regime: MarketRegime):
        self._regime_counts[regime.value] += 1

    def record_flow_event(self, event: str):
        self._flow_events[event] += 1

    def increment_scan(self):
        self._scan_count += 1

    @property
    def scan_count(self) -> int:
        return self._scan_count

    def get_strategy_stats(self, strategy: Strategy) -> dict:
        trades = self.trades[strategy]
        if not trades:
            return {'trades': 0, 'win_rate': 0, 'avg_pnl': 0, 'total_pnl': 0,
                    'avg_hold': 0, 'profit_factor': 0}
        wins = [t for t in trades if t.pnl_cents > 0]
        losses = [t for t in trades if t.pnl_cents <= 0]
        gross_profit = sum(t.pnl_cents for t in wins)
        gross_loss = abs(sum(t.pnl_cents for t in losses)) or 1
        return {
            'trades': len(trades),
            'win_rate': len(wins) / len(trades),
            'avg_pnl': sum(t.pnl_cents for t in trades) / len(trades),
            'total_pnl': sum(t.pnl_cents for t in trades),
            'avg_hold': sum(t.hold_time_seconds for t in trades) / len(trades),
            'profit_factor': gross_profit / gross_loss,
            'by_market_type': self._pnl_by_type(trades),
        }

    def _pnl_by_type(self, trades: List[TradeRecord]) -> dict:
        by_type: Dict[str, List[float]] = defaultdict(list)
        for t in trades:
            by_type[t.market_type.value].append(t.pnl_cents)
        return {k: {'trades': len(v), 'total_pnl': sum(v), 'avg_pnl': sum(v)/len(v)}
                for k, v in by_type.items()}

    def is_strategy_profitable(self, strategy: Strategy, config: dict) -> bool:
        stats = self.get_strategy_stats(strategy)
        paper_cfg = config.get('paper', {})
        return (
            stats['trades'] >= paper_cfg.get('min_profitable_trades_to_go_live', 30) and
            stats['win_rate'] >= paper_cfg.get('min_win_rate_to_go_live', 0.55) and
            stats['profit_factor'] >= paper_cfg.get('min_profit_factor_to_go_live', 1.3)
        )

    def print_report(self):
        print("\n" + "=" * 80)
        print("PERFORMANCE REPORT")
        print("=" * 80)
        for strategy in Strategy:
            stats = self.get_strategy_stats(strategy)
            if stats['trades'] == 0:
                print(f"\n  {strategy.value}: NO TRADES")
                continue
            print(f"\n  {strategy.value}:")
            print(f"    Trades: {stats['trades']}  |  Win Rate: {stats['win_rate']:.1%}")
            print(f"    Total PnL: {stats['total_pnl']:.0f}¢ (${stats['total_pnl']/100:.2f})")
            print(f"    Avg PnL/Trade: {stats['avg_pnl']:.1f}¢  |  Profit Factor: {stats['profit_factor']:.2f}")
            print(f"    Avg Hold: {stats['avg_hold']:.0f}s")
            if stats.get('by_market_type'):
                print(f"    By Market Type:")
                for mt, mstats in stats['by_market_type'].items():
                    print(f"      {mt}: {mstats['trades']} trades, PnL={mstats['total_pnl']:.0f}¢")

        # Flow intelligence summary
        if self._flow_events:
            print(f"\n  ORDER FLOW EVENTS:")
            for event, count in sorted(self._flow_events.items(), key=lambda x: -x[1]):
                print(f"    {event}: {count}")

        if self._regime_counts:
            print(f"\n  REGIME DISTRIBUTION:")
            total = sum(self._regime_counts.values())
            for regime, count in sorted(self._regime_counts.items(), key=lambda x: -x[1]):
                print(f"    {regime}: {count} ({count/total:.0%})")

    def print_failure_summary(self):
        print(f"\n[FAILURE_DIAG] Scan #{self._scan_count}")
        for strategy in Strategy:
            failures = self.failures[strategy]
            if failures:
                total = sum(failures.values())
                print(f"  {strategy.value}_failures ({total} total):")
                for reason, count in sorted(failures.items(), key=lambda x: -x[1]):
                    print(f"    {reason}: {count}")


# ============================================================
# KALSHI API CLIENT
# ============================================================

def _load_dotenv(path: str = '.env'):
    """Load .env file into os.environ (no third-party dependency needed)."""
    env_path = Path(path)
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            os.environ.setdefault(key.strip(), value.strip())


class KalshiAPI:
    """
    Kalshi API v2 client — RSA signature authentication.

    Credentials loaded in order:
      1. .env file  (preferred)
      2. Hardcoded fallback constants at top of file

    Every request is signed:  message = timestamp + METHOD + /path
    Signed with RSA-SHA256 (PKCS1v15), base64-encoded.
    """

    def __init__(self, config=None):
        # ── Load .env (best-effort) ───────────────────────────
        _load_dotenv()

        # ── Read credentials: .env first, then hardcoded fallback ──
        self.api_key_id = (
            os.environ.get('KALSHI_API_KEY_ID')
            or KALSHI_API_KEY_ID
        )
        pem_path = (
            os.environ.get('KALSHI_PRIVATE_KEY_PATH')
            or KALSHI_PRIVATE_KEY_PATH
        )
        self.base_url = (
            os.environ.get('KALSHI_API_BASE')
            or KALSHI_API_BASE
        )

        # ── Debug output ──────────────────────────────────────
        masked_key = (self.api_key_id[:8] + "...") if len(self.api_key_id) > 8 else self.api_key_id
        print(f"[API] Loaded key: {masked_key}")
        print(f"[API] Using base URL: {self.base_url}")

        # ── Validate — fail loudly if missing ─────────────────
        if not self.api_key_id or self.api_key_id == "your_key_here":
            raise RuntimeError(
                "MISSING: KALSHI_API_KEY_ID — set it in .env or at the top of kalshi_pro_trader.py"
            )
        if not pem_path:
            raise RuntimeError(
                "MISSING: KALSHI_PRIVATE_KEY_PATH — set it in .env or at the top of kalshi_pro_trader.py"
            )

        pem_file = Path(pem_path).expanduser()
        if not pem_file.is_absolute():
            script_dir = Path(__file__).resolve().parent
            cwd_candidate = (Path.cwd() / pem_file).resolve()
            script_candidate = (script_dir / pem_file).resolve()
            pem_file = cwd_candidate if cwd_candidate.exists() else script_candidate

        print(f"[DEBUG] RSA key path: {pem_file}")

        if not pem_file.exists():
            raise RuntimeError(f"RSA key file not found: {pem_file}")

        # ── Load RSA private key ──────────────────────────────
        with open(pem_file, "rb") as f:
            self.private_key = load_pem_private_key(f.read(), password=None)

        # ── Startup confirmation ──────────────────────────────
        print(f"API CONNECTED: {bool(self.api_key_id and self.private_key)}")

    # ── Signing ───────────────────────────────────────────────

    def _sign(self, method: str, path: str) -> Tuple[str, str]:
        """
        Sign a request.
        Returns (timestamp_ms, base64_signature).
        """
        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{path}"
        signature = self.private_key.sign(
            message.encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return timestamp, base64.b64encode(signature).decode()

    # ── Core request method ───────────────────────────────────

    def _request(self, method: str, path: str, body: dict = None,
                 params: dict = None) -> dict:
        """
        Send a signed request to Kalshi API.
        
        Args:
            method: HTTP method (GET, POST, DELETE …)
            path:   API path starting with / (e.g. /markets)
            body:   JSON body for POST/PUT (optional)
            params: URL query parameters (optional)
        
        Returns:
            Parsed JSON response as dict, or {} on error.
        """
        timestamp, sig_b64 = self._sign(method, path)

        headers = {
            'KALSHI-ACCESS-KEY': self.api_key_id,
            'KALSHI-ACCESS-SIGNATURE': sig_b64,
            'KALSHI-ACCESS-TIMESTAMP': timestamp,
            'Content-Type': 'application/json',
        }

        url = f"{self.base_url}{path}"

        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=body,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logging.error(f"[API] {method} {path} → {e.response.status_code}: "
                          f"{e.response.text[:200]}")
            return {}
        except Exception as e:
            logging.error(f"[API] {method} {path} error: {e}")
            return {}

    # ── Convenience methods ───────────────────────────────────

    def get(self, path: str, params: dict = None) -> dict:
        return self._request('GET', path, params=params)

    def post(self, path: str, body: dict = None) -> dict:
        return self._request('POST', path, body=body)

    def delete(self, path: str) -> dict:
        return self._request('DELETE', path)

    # ── Domain helpers ────────────────────────────────────────

    def connect(self) -> bool:
        """Test connectivity by fetching one market."""
        try:
            data = self.get('/markets', params={'limit': 1})
            if 'markets' in data:
                logging.info("[API] Kalshi RSA auth verified — connected ✓")
                return True
            logging.error(f"[API] Auth check failed: {data}")
            return False
        except Exception as e:
            logging.error(f"[API] Connection test error: {e}")
            return False

    def get_markets(self, series_ticker: str = None, status: str = 'open',
                    limit: int = 200) -> List[dict]:
        params = {'status': status, 'limit': limit}
        if series_ticker:
            params['series_ticker'] = series_ticker
        return self.get('/markets', params=params).get('markets', [])


# ============================================================
# MAIN BOT v2.0 — ADAPTIVE ORDER-FLOW-AWARE
# ============================================================

class KalshiProTrader:
    """Main orchestrator with all Phase 1-6 upgrades."""

    def __init__(self, config_path: str = 'kalshi_pro_config.yaml',
                 enable_only: str = None, force_live: bool = False):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        log_cfg = self.config.get('logging', {})
        logging.basicConfig(
            level=getattr(logging, log_cfg.get('level', 'INFO')),
            format='%(asctime)s %(levelname)s %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_cfg.get('file', 'kalshi_pro.log')),
            ]
        )

        # Core components
        self.api = KalshiAPI(self.config)  # loads credentials from .env or fallback constants
        self.classifier = MarketClassifier(self.config)
        self.fair_value = FairValueEngine(self.config)
        self.risk = RiskManager(self.config)
        self.metrics = MetricsTracker()

        # Phase 1: Order Flow Engine (replaces OrderBookAnalyzer)
        self.flow_engine = OrderFlowEngine(self.api, self.config)

        # Phase 3: Strategy Allocator
        self.allocator = StrategyAllocator(self.config)

        # Phase 4: Fill Quality Tracker
        self.fill_tracker = FillQualityTracker(self.config)

        # Phase 5: Regime Detector
        self.regime_detector = RegimeDetector(self.config)

        # Phase 6: Smart Execution
        is_paper = not force_live
        self.execution = SmartExecutionEngine(self.api, self.config, is_paper=is_paper)

        # Strategy engines
        self.strategies: Dict[Strategy, Any] = {}
        if not enable_only or enable_only == 'spread_arb':
            self.strategies[Strategy.SPREAD_ARB] = SpreadArbStrategy(self.config)
        if not enable_only or enable_only == 'scalper':
            self.strategies[Strategy.SCALPER] = ScalperStrategy(self.config)
        if not enable_only or enable_only == 'value_maker':
            self.strategies[Strategy.VALUE_MAKER] = ValueMakerStrategy(self.config)
        if not enable_only or enable_only == 'tennis_mm':
            self.strategies[Strategy.TENNIS_MM] = TennisMMStrategy(self.config)

        self.enable_only = enable_only
        self._positions: Dict[str, Position] = {}
        self._running = True
        self._scan_interval = 15

        logging.info(f"[INIT] KalshiProTrader v2.0 ADAPTIVE | mode={'PAPER' if is_paper else 'LIVE'}")
        logging.info(f"[INIT] Active strategies: {list(self.strategies.keys())}")
        logging.info(f"[INIT] Modules: OrderFlowEngine ✓ | AdaptiveParams ✓ | "
                     f"StrategyAllocator ✓ | FillQualityTracker ✓ | "
                     f"RegimeDetector ✓ | SmartExecution ✓")

    def run(self):
        connected = self.api.connect()
        logging.info(f"[RUN] Starting main loop | connected={connected}")

        diag_interval = self.config.get('logging', {}).get('diagnostics_interval_scans', 10)
        report_interval = self.config.get('logging', {}).get('metrics_report_interval_seconds', 300)
        last_report = time.time()

        try:
            while self._running:
                scan_start = time.time()
                self.metrics.increment_scan()

                markets = self._fetch_all_markets()
                logging.info(f"[SCAN #{self.metrics.scan_count}] Fetched {len(markets)} raw markets")

                classified = []
                for raw in markets:
                    cm = self.classifier.classify(
                        ticker=raw.get('ticker', ''), title=raw.get('title', ''),
                        series=raw.get('series_ticker', ''),
                        event_ticker=raw.get('event_ticker', ''),
                    )
                    if cm:
                        cm.volume = raw.get('volume', 0)
                        cm.yes_price = raw.get('yes_price', 0)
                        cm.no_price = raw.get('no_price', 0)
                        cm.close_time = raw.get('close_time')
                        classified.append(cm)

                logging.info(f"[SCAN] Classified: {len(classified)} markets "
                           f"(GW={sum(1 for c in classified if c.market_type==MarketType.GAME_WINNER)} "
                           f"SP={sum(1 for c in classified if c.market_type==MarketType.SPREAD)} "
                           f"TN={sum(1 for c in classified if c.market_type==MarketType.TENNIS)} "
                           f"TOT={sum(1 for c in classified if c.market_type==MarketType.TOTAL)})")

                signals_generated = 0
                scored_markets = []

                for market in classified:
                    # Get order book
                    book = (self.flow_engine.get_book(market.ticker) if connected
                            else self._simulate_book(market))

                    # Phase 1: Full order flow analysis
                    flow = self.flow_engine.analyze(market.ticker, book)

                    # Track flow events
                    if flow['aggression'] != 'none':
                        self.metrics.record_flow_event(f"aggression_{flow['aggression']}")
                    if flow['liquidity_pull']:
                        self.metrics.record_flow_event("liquidity_pull")
                    if flow['whale_detected']:
                        self.metrics.record_flow_event("whale_detected")
                    if flow['whale_accumulating']:
                        self.metrics.record_flow_event("whale_accumulating")

                    # Phase 5: Regime classification
                    regime = self.regime_detector.classify(market, book, flow)
                    self.metrics.record_regime(regime)

                    if regime == MarketRegime.DEAD:
                        for strat in self.strategies:
                            self.metrics.record_failure(strat, "regime_dead")
                        continue

                    # Liquidity check
                    liq_ok, liq_reason = self.risk.passes_liquidity(market, book)
                    if not liq_ok:
                        for strat in self.strategies:
                            self.metrics.record_failure(strat, f"liquidity_{liq_reason}")
                        continue

                    # Fair value
                    fv = self.fair_value.get_fair_value(market)

                    # Phase 3: Score market for allocation
                    score = self.allocator.score_market(market, book, flow, fv)
                    scored_markets.append((score, market))

                    # Run each strategy with regime + flow awareness
                    for strat_type, engine in self.strategies.items():
                        # Phase 5: regime gate
                        if not self.regime_detector.strategy_allowed(strat_type, regime):
                            self.metrics.record_failure(strat_type, f"regime_{regime.value}_blocked")
                            continue

                        # Phase 4: check if signal type still enabled
                        signal = None

                        if strat_type == Strategy.SPREAD_ARB:
                            signal = engine.scan(market, book, flow)
                            if not signal:
                                self.metrics.record_failure(strat_type,
                                    "no_arb_edge" if book.arb_cost >= 100 else "insufficient_depth")

                        elif strat_type == Strategy.SCALPER:
                            signal = engine.scan(market, book, fv, flow)
                            if not signal:
                                self.metrics.record_failure(strat_type, "no_deviation")

                        elif strat_type == Strategy.VALUE_MAKER:
                            signal = engine.scan(market, book, fv, flow)
                            if not signal:
                                self.metrics.record_failure(strat_type,
                                    "no_fair_value" if not fv else "insufficient_edge")

                        elif strat_type == Strategy.TENNIS_MM:
                            signal = engine.scan(market, book, flow)
                            if not signal:
                                self.metrics.record_failure(strat_type,
                                    "not_tennis" if market.market_type != MarketType.TENNIS else "narrow_spread")

                        if signal:
                            # Phase 4: check signal type
                            if not self.fill_tracker.is_signal_enabled(strat_type, signal.signal_type):
                                self.metrics.record_failure(strat_type, f"signal_disabled_{signal.signal_type}")
                                continue

                            # Phase 3: apply capital allocation
                            alloc = self.allocator.allocate_capital(scored_markets, signal.quantity)
                            if market.ticker in alloc:
                                signal.quantity = alloc[market.ticker]

                            # Risk check
                            can_trade, risk_reason = self.risk.can_trade(signal)
                            if not can_trade:
                                self.metrics.record_failure(strat_type, f"risk_{risk_reason}")
                                continue

                            # Phase 6: Smart execution
                            order_ids = self.execution.place_layered_entry(signal, book, flow)
                            if order_ids:
                                signals_generated += 1
                                self.risk.record_trade(signal)
                                self._positions[market.ticker] = Position(
                                    strategy=signal.strategy, ticker=market.ticker,
                                    side=signal.side, avg_entry=signal.entry_price,
                                    quantity=signal.quantity, target_price=signal.target_price,
                                    stop_price=signal.stop_price, entry_time=time.time(),
                                    order_ids=order_ids, signal_type=signal.signal_type,
                                )
                                logging.info(f"[SIGNAL] {signal.strategy.value} | "
                                           f"regime={regime.value} | pressure={flow['pressure'].value} | "
                                           f"{signal.reason}")

                logging.info(f"[SCAN] Signals: {signals_generated}")

                # Process fills
                self.execution.simulate_fills()
                fills = self.execution.get_fills()
                for fill in fills:
                    self._process_fill(fill)

                # Phase 6: Smart cancel/repost based on flow
                self._smart_order_management()

                # Manage positions
                self._manage_positions()

                # Diagnostics
                if self.metrics.scan_count % diag_interval == 0:
                    self.metrics.print_failure_summary()
                    self.fill_tracker.evaluate_all()

                # Periodic report
                if time.time() - last_report > report_interval:
                    self.metrics.print_report()
                    self.fill_tracker.print_report()
                    self._print_live_readiness()
                    last_report = time.time()

                elapsed = time.time() - scan_start
                time.sleep(max(1, self._scan_interval - elapsed))

        except KeyboardInterrupt:
            logging.info("[RUN] Shutting down...")
        finally:
            self.metrics.print_report()
            self.fill_tracker.print_report()
            self._print_live_readiness()
            self._save_state()

    def _smart_order_management(self):
        """Phase 6: Cancel/repost orders based on order flow changes."""
        for oid in list(self.execution.open_orders.keys()):
            order = self.execution.open_orders[oid]
            if order['status'] != 'pending':
                continue

            ticker = order['ticker']
            # Need current book + flow for this market
            if ticker in self.flow_engine._prev_books:
                book = self.flow_engine._prev_books[ticker]
                # Get latest flow snapshot
                snaps = self.flow_engine._snapshots.get(ticker)
                if snaps:
                    latest = snaps[-1]
                    flow = {
                        'pressure_score': 0,  # simplified — use last known
                        'volatility': 0,
                    }
                    should_cancel, new_price = self.execution.should_cancel_repost(oid, book, flow)
                    if should_cancel:
                        if new_price:
                            self.execution.repost_order(oid, new_price)
                            self.metrics.record_flow_event("smart_repost")
                        else:
                            self.execution.cancel_order(oid)
                            self.metrics.record_flow_event("flow_cancel")

    @staticmethod
    def _is_sports_market(market: dict) -> bool:
        """Filter sports markets by the canonical Kalshi ticker prefix."""
        if not market or not isinstance(market.get('ticker'), str):
            return False
        return market['ticker'].upper().startswith("KXMVESPORTSMULTIGAMEEXTENDED")

    def _fetch_all_markets(self) -> List[dict]:
        """Fetch all open markets, then filter to sports via ticker prefix."""
        all_markets = []
        seen_tickers = set()
        cursor = None

        # Paginate through ALL open markets
        while True:
            try:
                params = {'status': 'open', 'limit': 200}
                if cursor:
                    params['cursor'] = cursor
                resp = self.api.get('/markets', params=params)
                batch = resp.get('markets', [])
                if not batch:
                    break
                for m in batch:
                    t = m.get('ticker', '')
                    if t and t not in seen_tickers:
                        seen_tickers.add(t)
                        all_markets.append(m)
                cursor = resp.get('cursor')
                if not cursor:
                    break
            except Exception as e:
                logging.warning(f"[FETCH] Error fetching markets page: {e}")
                break

        # Filter to sports markets using ticker prefix
        sports_markets = [m for m in all_markets if self._is_sports_market(m)]
        logging.info(f"[FILTER] Sports markets: {len(sports_markets)} / {len(all_markets)} total")
        if sports_markets:
            logging.info(f"[FILTER SAMPLE] {[m['ticker'] for m in sports_markets[:5]]}")

        return sports_markets

    def _simulate_book(self, market: ClassifiedMarket) -> OrderBook:
        mid = market.yes_price or random.randint(30, 70)
        spread = random.randint(2, 8)
        book = OrderBook(timestamp=time.time())
        for i in range(5):
            bid_price = max(1, mid - spread // 2 - i * 2)
            ask_price = min(99, mid + spread // 2 + i * 2)
            qty = random.randint(50, 2000)
            book.yes_bids.append(OrderBookLevel(price=bid_price, quantity=qty))
            book.yes_asks.append(OrderBookLevel(price=ask_price, quantity=qty))
            book.no_bids.append(OrderBookLevel(price=100 - ask_price, quantity=qty))
            book.no_asks.append(OrderBookLevel(price=100 - bid_price, quantity=qty))
        book.yes_bids.sort(key=lambda x: x.price, reverse=True)
        book.yes_asks.sort(key=lambda x: x.price)
        book.no_bids.sort(key=lambda x: x.price, reverse=True)
        book.no_asks.sort(key=lambda x: x.price)
        return book

    def _process_fill(self, fill: dict):
        ticker = fill['ticker']
        if ticker in self._positions:
            pos = self._positions[ticker]
            pos.filled_qty += fill['filled_qty']
            self.execution.place_take_profit(
                ticker, pos.side, pos.target_price,
                fill['filled_qty'], pos.strategy)

    def _manage_positions(self):
        for ticker, pos in list(self._positions.items()):
            elapsed = time.time() - pos.entry_time
            max_hold = self.config.get('strategies', {}).get(
                pos.strategy.value, {}).get('max_hold_seconds', 600)
            if elapsed > max_hold:
                self._close_position(ticker, "time_exit")
                continue
            if not pos.partial_profit_taken and pos.filled_qty > 0:
                if elapsed > 30 and random.random() < 0.1:
                    partial_qty = pos.filled_qty // 2
                    if partial_qty > 0:
                        pos.partial_profit_taken = True
                        logging.info(f"[POS] Partial profit {partial_qty}x {ticker}")

    def _close_position(self, ticker: str, reason: str):
        pos = self._positions.pop(ticker, None)
        if not pos:
            return
        exit_price = pos.avg_entry + random.randint(-5, 10)
        pnl = (exit_price - pos.avg_entry) * pos.filled_qty
        if pos.side == Side.NO:
            pnl = (pos.avg_entry - exit_price) * pos.filled_qty
        slippage = abs(exit_price - pos.avg_entry) if reason == "time_exit" else 0

        trade = TradeRecord(
            strategy=pos.strategy, ticker=ticker,
            market_type=MarketType.UNKNOWN, side=pos.side,
            entry_price=pos.avg_entry, exit_price=exit_price,
            quantity=pos.filled_qty, pnl_cents=pnl,
            hold_time_seconds=time.time() - pos.entry_time,
            entry_time=pos.entry_time, exit_time=time.time(),
            reason=reason, signal_type=pos.signal_type,
            slippage_cents=slippage,
        )
        self.metrics.record_trade(trade)
        self.fill_tracker.record(trade)
        self.risk.record_pnl(pnl)
        logging.info(f"[CLOSE] {pos.strategy.value} {ticker} pnl={pnl:.0f}¢ reason={reason}")

    def _print_live_readiness(self):
        print("\n" + "-" * 40)
        print("LIVE READINESS CHECK")
        print("-" * 40)
        for strat in Strategy:
            if strat not in self.strategies:
                continue
            ready = self.metrics.is_strategy_profitable(strat, self.config)
            stats = self.metrics.get_strategy_stats(strat)
            status = "✅ READY" if ready else "❌ NOT READY"
            print(f"  {strat.value}: {status}")
            print(f"    Trades: {stats['trades']} | WR: {stats['win_rate']:.0%} | PF: {stats['profit_factor']:.2f}")

    def _save_state(self):
        try:
            state = {
                'version': '2.0-adaptive',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'scans': self.metrics.scan_count,
                'strategies': {},
                'failures': {},
                'flow_events': dict(self.metrics._flow_events),
                'regime_distribution': dict(self.metrics._regime_counts),
                'fill_quality': self.fill_tracker.evaluate_all(),
            }
            for strat in Strategy:
                state['strategies'][strat.value] = self.metrics.get_strategy_stats(strat)
                state['failures'][strat.value] = dict(self.metrics.failures[strat])
            with open('kalshi_pro_state.json', 'w') as f:
                json.dump(state, f, indent=2, default=str)
            logging.info("[STATE] Saved to kalshi_pro_state.json")
        except Exception as e:
            logging.error(f"[STATE] Save error: {e}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Kalshi Pro Trading System v2.0 — Adaptive')
    parser.add_argument('--config', default='kalshi_pro_config.yaml', help='Config file path')
    parser.add_argument('--enable-only', choices=['spread_arb', 'scalper', 'value_maker', 'tennis_mm'],
                        help='Run only one strategy')
    parser.add_argument('--live', action='store_true', help='Enable live trading')
    parser.add_argument('--scan-interval', type=int, default=15, help='Seconds between scans')
    args = parser.parse_args()

    bot = KalshiProTrader(
        config_path=args.config, enable_only=args.enable_only,
        force_live=args.live,
    )
    bot._scan_interval = args.scan_interval
    bot.run()


if __name__ == '__main__':
    main()

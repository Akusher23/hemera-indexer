from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Sequence

from pydantic import BaseModel
from sqlmodel import Session, select

from hemera.common.models.prices import CoinPrices, TokenHourlyPrices, TokenPrices
from hemera.common.models.tokens import Tokens
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes


class TokenInfo(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    symbol: Optional[str] = None
    logo_url: Optional[str] = None
    type: Optional[str] = None
    decimals: Optional[int] = None

    @classmethod
    def from_db_model(cls, token: Tokens) -> "TokenInfo":
        return cls(
            name=token.name,
            address=bytes_to_hex_str(token.address),
            symbol=token.symbol,
            type=token.token_type,
            decimals=token.decimals,
            logo_url=token.logo_url,
        )


def get_token_price(session: Session, symbol: str, date: Optional[datetime] = None) -> Decimal:
    """Get token price

    Args:
        session: SQLModel session
        symbol: Token symbol
        date: Optional date to get token price at that date, defaults to None will get the latest price

    Returns:
        Decimal: Token price
    """
    if date:
        statement = (
            select(TokenHourlyPrices)
            .where(TokenHourlyPrices.symbol == symbol, TokenHourlyPrices.timestamp <= date)
            .order_by(TokenHourlyPrices.timestamp.desc())
        )
    else:
        statement = select(TokenPrices).where(TokenPrices.symbol == symbol).order_by(TokenPrices.timestamp.desc())

    token_price = session.exec(statement).first()
    return token_price.price if token_price else Decimal(0.0)


def get_coin_prices(session: Session, dates: List[datetime]) -> List[CoinPrices]:
    """Get coin prices for specified dates

    Args:
        session: SQLModel session
        dates: List of dates to query prices for

    Returns:
        List[CoinPrices]: List of coin prices
    """
    statement = select(CoinPrices).where(CoinPrices.block_date.in_(dates))

    coin_prices = session.exec(statement).all()
    return coin_prices


def get_latest_coin_price(session: Session) -> float:
    """Get latest coin price

    Args:
        session: SQLModel session

    Returns:
        float: Latest coin price, 0.0 if no price found
    """
    statement = select(CoinPrices).order_by(CoinPrices.block_date.desc())

    result = session.exec(statement).first()
    return float(result.price) if result and result.price else 0.0


def get_tokens_by_token_address(session: Session, token_addresses: List[str]) -> List[TokenInfo]:
    """Get token info map for specified token addresses

    Args:
        session: SQLModel session
        token_addresses: List of token addresses
    Returns:
        List[TokenInfo]: List of token info
    """
    address_set = set()
    for address in token_addresses:
        if isinstance(address, str):
            address_set.add(hex_str_to_bytes(address))
        else:
            address_set.add(address)
    statement = select(Tokens).where(Tokens.address.in_(address_set))
    tokens = session.exec(statement).all()
    return [TokenInfo.from_db_model(token) for token in tokens]

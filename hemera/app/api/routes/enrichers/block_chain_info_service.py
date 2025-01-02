from typing import Any, Callable, Dict, List, Set, Type, TypeVar

from sqlmodel import Session

from hemera.app.api.routes.enrichers import BaseFormatter
from hemera.app.api.routes.enrichers.formatter.internal_transaction_formatter import InternalTransactionFormatter
from hemera.app.api.routes.enrichers.formatter.token_transfer_formatter import TokenTransferFormatter
from hemera.app.api.routes.enrichers.formatter.transaction_formatter import TransactionFormatter
from hemera.app.api.routes.enrichers.mapper.contract_mapper import ContractMapper
from hemera.app.api.routes.enrichers.mapper.tag_mapper import TagMapper
from hemera.app.api.routes.enrichers.mapper.token_mapper import TokenMapper
from hemera.app.api.routes.helper.internal_transaction import InternalTransactionAbbr
from hemera.app.api.routes.helper.transaction import TransactionAbbr
from hemera.indexer.domains.token_transfer import TokenTransfer

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class BlockchainEnricher:
    """Central service for blockchain data enrichment"""

    def __init__(self, session: Session):
        # Initialize mappers with default refresh interval
        self.token_mapper = TokenMapper(session)
        self.contract_mapper = ContractMapper(session)
        self.tag_mapper = TagMapper(session)

        # Register formatters
        self._formatters: Dict[Type, BaseFormatter] = {
            TokenTransfer: TokenTransferFormatter(),
            InternalTransactionAbbr: InternalTransactionFormatter(),
            TransactionAbbr: TransactionFormatter(),
        }

    def _collect_keys(self, items: List[Any], key_extractors: Dict[str, Callable]) -> Dict[str, Set]:
        """Collect all required keys for mapping"""
        keys = {name: set() for name in key_extractors}
        for item in items:
            for name, extractor in key_extractors.items():
                extracted = extractor(item)
                if isinstance(extracted, (list, set)):
                    keys[name].update(extracted)
                else:
                    keys[name].add(extracted)
        return keys

    def enrich(
        self, items: List[T], response_model: Type[V], key_extractors: Dict[str, Callable], force_refresh: bool = False
    ) -> List[V]:
        """
        Enrich items with specified mappings

        Args:
            items: List of items to enrich
            response_model: Pydantic model for response
            key_extractors: Dict of name -> function to extract keys from items
            force_refresh: Whether to force cache refresh

        Returns:
            List of enriched items in response_model format
        """
        if not items:
            return []

        # Collect all required keys
        mapping_keys = self._collect_keys(items, key_extractors)

        # Prepare context with all required mappings
        context = {
            "token_map": self.token_mapper.get_mapping(mapping_keys.get("token", set()), force_refresh),
            "contract_map": self.contract_mapper.get_mapping(mapping_keys.get("contract", set()), force_refresh),
            "tags_map": self.tag_mapper.get_mapping(mapping_keys.get("address", set()), force_refresh),
            "ens_map": {},  # TODO: Add ENS mapping
        }

        # Format items
        formatter = self._formatters[type(items[0])]
        formatted_items = formatter.format_batch(items, context)

        # Convert to response model
        return [response_model(**item) for item in formatted_items]


class EnricherManager:
    _instance = None
    _enricher = None

    @classmethod
    def get_instance(cls) -> "EnricherManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_enricher(self, session: Session) -> BlockchainEnricher:
        if self._enricher is None:
            self._enricher = BlockchainEnricher(session)
        else:
            self._enricher.session = session
            self._enricher.token_mapper.session = session
            self._enricher.contract_mapper.session = session
        return self._enricher
